use std::{fs, sync::Arc};

use airbyte_protocol::message::{
    AirbyteMessage, ConfiguredAirbyteCatalog, ConfiguredAirbyteStream, DestinationSyncMode,
    SyncMode,
};
use airbyte_protocol::schema::{Compound, JsonSchema, Type};
use anyhow::anyhow;
use futures::{stream, StreamExt, TryStreamExt};
use iceberg_rust::spec::partition::{PartitionField, PartitionSpec, Transform};
use iceberg_rust::table::Table;
use iceberg_rust::{catalog::identifier::Identifier, spec::schema::Schema};

use crate::{error::Error, plugin::DestinationPlugin, schema::schema_to_arrow};

pub async fn configure_catalog(
    path: &str,
    plugin: Arc<dyn DestinationPlugin>,
) -> Result<ConfiguredAirbyteCatalog, Error> {
    let json = fs::read_to_string(path)?;

    let streams = plugin.streams();

    let message: AirbyteMessage = serde_json::from_str(&json)?;

    let AirbyteMessage::Catalog { catalog } = message else {
        return Err(Error::Anyhow(anyhow!("Not a catalog message")));
    };

    let streams = stream::iter(catalog.streams.into_iter())
        .then(|mut stream| {
            let plugin = plugin.clone();
            async move {
                let namespace = plugin
                    .namespace()
                    .or(stream.namespace.as_deref())
                    .map(ToOwned::to_owned)
                    .unwrap_or("public".to_owned());
                let ident = Identifier::new(&[namespace], &stream.name);

                let config = streams.get(&ident.to_string());

                let cursor_field = config
                    .and_then(|x| x.cursor_field.as_ref())
                    .or(stream.default_cursor_field.as_ref())
                    .cloned();
                let sync_mode = match (
                    config.and_then(|x| x.sync_mode.as_ref()),
                    stream.supported_sync_modes.contains(&SyncMode::Incremental),
                ) {
                    (Some(SyncMode::FullRefresh), true) => SyncMode::FullRefresh,
                    (_, true) => SyncMode::Incremental,
                    _ => SyncMode::FullRefresh,
                };
                let destination_sync_mode = match &sync_mode {
                    SyncMode::FullRefresh => DestinationSyncMode::Overwrite,
                    SyncMode::Incremental => DestinationSyncMode::Append,
                };

                if let DestinationSyncMode::Overwrite = destination_sync_mode {
                    remove_cdc_columns(&mut stream.json_schema)?;
                }

                let catalog = plugin.catalog().await?;

                if !catalog.tabular_exists(&ident).await? {
                    let arrow_schema = schema_to_arrow(&stream.json_schema)?;

                    let schema = Schema::builder()
                        .with_fields((&arrow_schema).try_into()?)
                        .build()
                        .map_err(iceberg_rust::spec::error::Error::from)?;

                    let base_path = plugin
                        .bucket()
                        .unwrap_or("")
                        .trim_end_matches("/")
                        .to_string()
                        + "/"
                        + &ident.to_string().replace(".", "/");

                    let mut builder = Table::builder();
                    builder.with_name(ident.name());

                    if let Some(columns) = config.and_then(|x| x.partition_by.as_ref()) {
                        builder.with_partition_spec(
                            PartitionSpec::builder()
                                .with_fields(
                                    columns
                                        .iter()
                                        .enumerate()
                                        .map(|(i, (column, transform))| {
                                            let field = schema.fields().get_name(column).ok_or(
                                                Error::Anyhow(anyhow!(
                                                    "Field {} doesn't exist in schema.",
                                                    column
                                                )),
                                            )?;
                                            let transform =
                                                serde_json::from_str::<Transform>(transform)?;
                                            Ok::<_, Error>(PartitionField::new(
                                                field.id,
                                                1000 + i as i32,
                                                column,
                                                transform,
                                            ))
                                        })
                                        .collect::<Result<_, _>>()?,
                                )
                                .build()
                                .map_err(iceberg_rust::spec::error::Error::from)?,
                        );
                    }

                    builder.with_location(&base_path).with_schema(schema);

                    builder.build(ident.namespace(), catalog).await?;
                }

                Ok::<_, Error>(ConfiguredAirbyteStream {
                    cursor_field,
                    destination_sync_mode,
                    generation_id: None,
                    minimum_generation_id: None,
                    primary_key: stream.source_defined_primary_key.clone(),
                    stream,
                    sync_id: None,
                    sync_mode,
                })
            }
        })
        .try_collect::<Vec<ConfiguredAirbyteStream>>()
        .await?;

    Ok(ConfiguredAirbyteCatalog { streams })
}

fn remove_cdc_columns(schema: &mut JsonSchema) -> Result<(), Error> {
    match &mut schema.r#type {
        Type::Compound(Compound::Object(object)) => {
            let keys: Vec<String> = object.properties.keys().map(ToOwned::to_owned).collect();
            for key in keys {
                if key.starts_with("_ab_cdc") {
                    object.properties.remove(key.as_str());
                }
            }
            Ok(())
        }
        _ => Ok(()),
    }
}
