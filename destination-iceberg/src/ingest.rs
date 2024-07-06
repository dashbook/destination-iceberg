use std::{collections::HashMap, io::BufRead, sync::Arc};

use airbyte_rs::message::{
    AirbyteGlobalState, AirbyteMessage, AirbyteStateMessage, ConfiguredAirbyteCatalog,
    DestinationSyncMode, StreamDescriptor,
};
use anyhow::anyhow;
use arrow::{datatypes::Schema as ArrowSchema, error::ArrowError, json::ReaderBuilder};
use futures::{
    channel::mpsc::{unbounded, UnboundedReceiver, UnboundedSender},
    lock::Mutex,
    stream, SinkExt, StreamExt, TryStreamExt,
};
use iceberg_rust::{
    arrow::write::write_parquet_partitioned,
    catalog::{identifier::Identifier, tabular::Tabular},
};

use tracing::{debug, debug_span, Instrument};

use crate::{
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
                    stream,
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

    let global_state: Arc<Mutex<Option<AirbyteGlobalState>>> = Arc::new(Mutex::new(None));
    let stream_states: Arc<Mutex<HashMap<_, serde_json::Value>>> =
        Arc::new(Mutex::new(HashMap::new()));

    // Process messages for every stream
    let handle = stream::iter(message_recievers.into_iter())
        .map(Ok::<_, Error>)
        .try_for_each_concurrent(None, |(stream, messages)| {
            let plugin = plugin.clone();
            let schemas = schemas.clone();
            let global_state = global_state.clone();
            let stream_states = stream_states.clone();
            async move {
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
                        .ok_or(Error::NotFound("Namespace".to_owned()))?
                        .to_string()],
                    &stream.name,
                );

                let table = catalog.clone().load_tabular(&ident).await?;

                let mut table = if let Tabular::Table(table) = table {
                    table
                } else {
                    return Err(Error::Unknown);
                };

                let table_schema = table
                    .metadata()
                    .current_schema(plugin.branch().as_deref())?;

                let table_arrow_schema: Arc<ArrowSchema> =
                    Arc::new((table_schema.fields()).try_into()?);

                let batches = messages
                    .filter_map(|message| async move {
                        match message {
                            AirbyteMessage::Record { record } => Some(record),
                            _ => None,
                        }
                    })
                    // Check if record conforms to schema
                    .map(|message| {
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
                    .and_then(|batches| {
                        let table_arrow_schema = table_arrow_schema.clone();
                        async move {
                            let mut decoder =
                                ReaderBuilder::new(table_arrow_schema.clone()).build_decoder()?;
                            decoder.serialize(&batches)?;
                            let record_batch = decoder.flush()?.ok_or(ArrowError::MemoryError(
                                "Data of recordbatch is empty.".to_string(),
                            ))?;
                            Ok(record_batch)
                        }
                    });

                let files = write_parquet_partitioned(
                    table.metadata(),
                    batches,
                    table.object_store(),
                    plugin.branch().as_deref(),
                )
                .await?;

                if !files.is_empty() {
                    let transaction = match schema.destination_sync_mode {
                        DestinationSyncMode::Overwrite => Ok(table
                            .new_transaction(plugin.branch().as_deref())
                            .rewrite(files)),
                        DestinationSyncMode::Append => Ok(table
                            .new_transaction(plugin.branch().as_deref())
                            .append(files)),
                        DestinationSyncMode::AppendDedup => {
                            Err(Error::Invalid("Sync Mode".to_owned()))
                        }
                    }?;

                    let (shared_state, global_stream_state) = {
                        let global_state = global_state.lock().await;
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

                    let stream_state = {
                        let stream_states = stream_states.lock().await;
                        stream_states.get(&stream).cloned()
                    };

                    if let Some(state) = &stream_state {
                        debug!("State of stream {}: {}", &stream.name, &state);
                    }
                    if let Some(state) = &global_stream_state {
                        debug!("Global state of stream {}: {}", &stream.name, &state);
                    }
                    if let Some(state) = &shared_state {
                        debug!("Global state : {}", &state);
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
                }

                Ok(())
            }
            .instrument(debug_span!("sync_stream"))
        });

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
                        let mut state = global_state.lock().await;
                        *state = Some(global.clone());
                    }
                    AirbyteStateMessage::Stream {
                        stream,
                        destination_stats: _,
                        source_stats: _,
                    } => {
                        if let Some(stream_state) = &stream.stream_state {
                            let mut stream_states = stream_states.lock().await;
                            stream_states
                                .insert(stream.stream_descriptor.clone(), stream_state.clone());
                        }
                    }
                    _ => (),
                },
                _ => (),
            }
        }
    }

    for (_, sender) in message_senders {
        sender.close_channel();
    }

    handle.await?;

    Ok(())
}
