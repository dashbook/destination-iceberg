use std::{collections::HashMap, sync::Arc};

use airbyte_protocol::message::{
    AirbyteCatalog, AirbyteGlobalState, AirbyteStateMessage, AirbyteStreamState, StreamDescriptor,
};
use futures::{lock::Mutex, stream, StreamExt, TryStreamExt};
use iceberg_rust::catalog::{identifier::Identifier, tabular::Tabular};

use crate::{error::Error, plugin::DestinationPlugin};

pub(crate) static AIRBYTE_SHARED_STATE: &str = "airbyte.shared_state";
pub(crate) static AIRBYTE_STREAM_STATE: &str = "airbyte.stream_state";

pub async fn generate_state(
    plugin: Arc<dyn DestinationPlugin>,
    airbyte_catalog: &AirbyteCatalog,
) -> Result<AirbyteStateMessage, Error> {
    let shared_state: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let stream_states: Arc<Mutex<HashMap<StreamDescriptor, String>>> =
        Arc::new(Mutex::new(HashMap::new()));

    stream::iter(airbyte_catalog.streams.iter())
        .map(Ok::<_, Error>)
        .try_for_each_concurrent(None, |stream| {
            let shared_state = shared_state.clone();
            let stream_states = stream_states.clone();
            let plugin = plugin.clone();
            async move {
                let namespace = plugin
                    .namespace()
                    .or(stream.namespace.as_deref())
                    .map(ToOwned::to_owned)
                    .unwrap_or("public".to_owned());
                let ident = Identifier::new(&[namespace], &stream.name);

                let catalog = plugin.catalog().await?;

                let table = catalog.load_tabular(&ident).await?;

                let table = if let Tabular::Table(table) = table {
                    table
                } else {
                    return Err(Error::Unknown);
                };

                if let Some(new) = table.metadata().properties.get(AIRBYTE_SHARED_STATE) {
                    let mut shared_state = shared_state.lock().await;
                    if (*shared_state).is_none() {
                        *shared_state = Some(new.clone())
                    }
                };
                if let Some(stream_state) = table.metadata().properties.get(AIRBYTE_STREAM_STATE) {
                    stream_states.lock().await.insert(
                        StreamDescriptor::new(ident.name(), Some(&ident.namespace()[0])),
                        stream_state.clone(),
                    );
                };
                Ok(())
            }
        })
        .await?;

    Ok(AirbyteStateMessage::Global {
        global: AirbyteGlobalState {
            shared_state: Arc::try_unwrap(shared_state)
                .unwrap()
                .into_inner()
                .map(|x| serde_json::from_str(&x))
                .transpose()?,
            stream_states: Arc::try_unwrap(stream_states)
                .unwrap()
                .into_inner()
                .into_iter()
                .map(|(key, value)| AirbyteStreamState {
                    stream_descriptor: key,
                    stream_state: serde_json::from_str(&value).unwrap(),
                })
                .collect(),
        },
        destination_stats: None,
        source_stats: None,
    })
}
