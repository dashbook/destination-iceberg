use std::{
    collections::HashMap,
    io::BufRead,
    sync::{Arc, Mutex, RwLock},
};

use airbyte_protocol::message::{
    AirbyteGlobalState, AirbyteMessage, AirbyteStateMessage, ConfiguredAirbyteCatalog,
    DestinationSyncMode, StreamDescriptor,
};
use anyhow::anyhow;
use arrow::{datatypes::Schema as ArrowSchema, error::ArrowError, json::ReaderBuilder};
use futures::{
    channel::mpsc::{unbounded, UnboundedReceiver, UnboundedSender},
    SinkExt, StreamExt, TryStreamExt,
};
use iceberg_rust::{
    arrow::write::write_parquet_partitioned,
    catalog::{identifier::Identifier, tabular::Tabular},
};

use tokio::task::JoinSet;
use tracing::{debug, debug_span, Instrument};

use crate::{
    catalog::DEFAULT_NAMESPACE,
    error::Error,
    plugin::DestinationPlugin,
    state::{AIRBYTE_SHARED_STATE, AIRBYTE_STREAM_STATE},
};

static ARROW_BATCH_SIZE: usize = 8192;

pub async fn ingest(
    input: &mut dyn BufRead,
    plugin: Arc<dyn DestinationPlugin>,
    airbyte_catalog: &ConfiguredAirbyteCatalog,
) -> Result<(), Error> {
    let schemas: Arc<HashMap<_, _>> = Arc::new(
        airbyte_catalog
            .streams
            .iter()
            .map(|stream| {
                (
                    StreamDescriptor::new(&stream.stream.name, stream.stream.namespace.as_deref()),
                    stream.clone(),
                )
            })
            .collect(),
    );

    // Create sender and reviever for every stream
    let (mut message_senders, message_recievers): (
        HashMap<StreamDescriptor, UnboundedSender<AirbyteMessage>>,
        Vec<(StreamDescriptor, UnboundedReceiver<AirbyteMessage>)>,
    ) = schemas
        .keys()
        .map(|key| {
            let (s, r) = unbounded();
            ((key.clone(), s), (key.clone(), r))
        })
        .unzip();

    let global_state: Arc<RwLock<Option<AirbyteGlobalState>>> = Arc::new(RwLock::new(None));

    let mut set = JoinSet::new();

    for (stream, messages) in message_recievers {
        set.spawn({
            let plugin = plugin.clone();
            let schemas = schemas.clone();
            let global_state = global_state.clone();
            async move {
                let stream_state = Arc::new(Mutex::new(None));

                let schema = schemas.get(&stream).ok_or(Error::NotFound(format!(
                    "Schema for stream {}",
                    &stream.name
                )))?;

                debug!("Syncing stream {}", &stream.name);
                debug!(
                    "Schema: {}",
                    serde_json::to_string(&schema.stream.json_schema)?
                );

                let compiled_schema = jsonschema::JSONSchema::compile(&serde_json::to_value(
                    &schema.stream.json_schema,
                )?)
                .unwrap();

                let catalog = plugin.catalog().await?;

                let ident = Identifier::new(
                    &[plugin
                        .namespace()
                        .or(stream.namespace.as_deref())
                        .unwrap_or(DEFAULT_NAMESPACE)
                        .to_string()],
                    &stream.name,
                );

                let table = catalog.clone().load_tabular(&ident).await?;

                let mut table = if let Tabular::Table(table) = table {
                    table
                } else {
                    return Err(Error::Unknown);
                };

                let table_schema = table.metadata().current_schema(plugin.branch())?;

                let table_arrow_schema: Arc<ArrowSchema> =
                    Arc::new((table_schema.fields()).try_into()?);

                let move_stream_state = stream_state.clone();

                let batches = messages
                    .filter_map(move |message| {
                        let stream_state = move_stream_state.clone();
                        async move {
                            match message {
                                AirbyteMessage::Record { record } => Some(record),
                                AirbyteMessage::State { state } => {
                                    if let AirbyteStateMessage::Stream {
                                        stream,
                                        destination_stats: _,
                                        source_stats: _,
                                    } = state
                                    {
                                        let mut stream_state = stream_state.lock().unwrap();
                                        *stream_state = stream.stream_state;
                                        None
                                    } else {
                                        None
                                    }
                                }
                                _ => None,
                            }
                        }
                    })
                    // Check if record conforms to schema
                    .map(move |message| {
                        compiled_schema.validate(&message.data).map_err(|mut err| {
                            let error =
                                format!("{} in data {}", err.next().unwrap(), &message.data);
                            Error::Anyhow(anyhow::Error::msg(error))
                        })?;
                        let value = message.data;

                        Ok::<_, Error>(value)
                    })
                    .try_chunks(ARROW_BATCH_SIZE)
                    .map_err(|err| ArrowError::ExternalError(Box::new(err)))
                    // Convert messages to arrow batches
                    .and_then(move |batches| {
                        let table_arrow_schema = table_arrow_schema.clone();
                        async move {
                            let mut decoder =
                                ReaderBuilder::new(table_arrow_schema).build_decoder()?;
                            decoder.serialize(&batches)?;
                            let record_batch = decoder.flush()?.ok_or(ArrowError::MemoryError(
                                "Data of recordbatch is empty.".to_string(),
                            ))?;
                            Ok(record_batch)
                        }
                    });

                let files = write_parquet_partitioned(&table, batches, plugin.branch()).await?;

                debug!("Stream {} finished writing parquet files.", &stream.name);

                if !files.is_empty() {
                    let transaction = match schema.destination_sync_mode {
                        DestinationSyncMode::Overwrite => {
                            Ok(table.new_transaction(plugin.branch()).replace(files))
                        }
                        DestinationSyncMode::Append => {
                            Ok(table.new_transaction(plugin.branch()).append_data(files))
                        }
                        DestinationSyncMode::AppendDedup => {
                            Err(Error::Invalid("Sync Mode".to_owned()))
                        }
                    }?;

                    let (shared_state, global_stream_state) = {
                        let global_state = global_state.read().unwrap();
                        if let Some(global) = global_state.as_ref() {
                            (
                                global.shared_state.clone(),
                                global
                                    .stream_states
                                    .iter()
                                    .find(|x| x.stream_descriptor == stream)
                                    .and_then(|x| x.stream_state.clone()),
                            )
                        } else {
                            (None, None)
                        }
                    };

                    if let Some(state) = &global_stream_state {
                        debug!("Global state of stream {}: {}", &stream.name, &state);
                    }
                    if let Some(state) = &shared_state {
                        debug!("Global state : {}", &state);
                    }

                    let stream_state = { stream_state.lock().unwrap().clone() };

                    if let Some(state) = &stream_state {
                        debug!("State of stream {}: {}", &stream.name, &state);
                    }

                    let transaction = match shared_state {
                        Some(x) => transaction.update_properties(vec![(
                            AIRBYTE_SHARED_STATE.to_string(),
                            x.to_string(),
                        )]),
                        None => transaction,
                    };
                    let transaction = match (stream_state, global_stream_state) {
                        (Some(x), _) => transaction.update_properties(vec![(
                            AIRBYTE_STREAM_STATE.to_string(),
                            x.to_string(),
                        )]),
                        (None, Some(x)) => transaction.update_properties(vec![(
                            AIRBYTE_STREAM_STATE.to_string(),
                            x.to_string(),
                        )]),
                        (None, None) => transaction,
                    };

                    transaction.commit().await?;

                    debug!("Stream {} finished table transaction.", &stream.name);
                }

                Ok(())
            }
            .instrument(debug_span!("sync_stream"))
        });
    }

    // Send messages to channel based on stream
    for line in input.lines() {
        let line = line?;

        if line.starts_with('{') && line.ends_with('}') {
            let message: AirbyteMessage = serde_json::from_str(&line)?;
            match &message {
                AirbyteMessage::Record { record } => {
                    message_senders
                        .get_mut(&StreamDescriptor::new(
                            &record.stream,
                            record.namespace.as_deref(),
                        ))
                        .ok_or(Error::Anyhow(anyhow!(
                            "Stream {} not found.",
                            &record.stream,
                        )))?
                        .send(message)
                        .await?
                }
                AirbyteMessage::State { state } => match state {
                    AirbyteStateMessage::Global {
                        global,
                        destination_stats: _,
                        source_stats: _,
                    } => {
                        let mut state = global_state.write().unwrap();
                        *state = Some(global.clone());
                    }
                    AirbyteStateMessage::Stream {
                        stream,
                        destination_stats: _,
                        source_stats: _,
                    } => {
                        message_senders
                            .get_mut(&stream.stream_descriptor)
                            .ok_or(Error::Anyhow(anyhow!(
                                "Stream {} not found.",
                                &stream.stream_descriptor.name,
                            )))?
                            .send(message)
                            .await?
                    }
                    _ => (),
                },
                _ => (),
            }
        }
    }

    debug!("Input stream finished.");

    for (_, sender) in message_senders {
        sender.close_channel();
    }

    while let Some(res) = set.join_next().await {
        res??;
    }

    debug!("All tasks joined successfully.");

    Ok(())
}
