use std::{
    fs,
    io::{self, BufReader},
    sync::Arc,
};

use airbyte_protocol::message::{AirbyteMessage, ConfiguredAirbyteCatalog};
use anyhow::anyhow;
use clap::Parser;
use destination_iceberg::{
    catalog::configure_catalog, error::Error, ingest::ingest, state::generate_state,
};
use plugin::SqlDestinationPlugin;
use tracing::{debug, info};
use tracing_subscriber::EnvFilter;

mod plugin;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None, rename_all = "kebab-case")]
struct Args {
    /// Path to the config file
    #[arg(long, default_value = "destination.json")]
    config: String,
    /// Generate state
    #[arg(long)]
    state: bool,
    /// Generate state
    #[arg(long)]
    write: bool,
    /// Configure catalog
    #[arg(long)]
    catalog: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let args = Args::parse();

    let plugin = Arc::new(SqlDestinationPlugin::new(&args.config).await?);

    if args.state {
        info!("Generating state");

        let catalog_json =
            fs::read_to_string(&args.catalog.ok_or(Error::NotFound("Catalog".to_owned()))?)?;

        let AirbyteMessage::Catalog { catalog } = serde_json::from_str(&catalog_json)? else {
            return Err(Error::Invalid("Catalog message".to_owned()));
        };

        let state = generate_state(plugin.clone(), &catalog).await?;

        let json = serde_json::to_string(&state)?;

        debug!("{}", &json);

        print!("{}", &json);

        Ok(())
    } else if args.write {
        info!("Start syncing ...");

        let catalog_json =
            fs::read_to_string(&args.catalog.ok_or(Error::NotFound("Catalog".to_owned()))?)?;

        let catalog: ConfiguredAirbyteCatalog = serde_json::from_str(&catalog_json)?;

        ingest(&mut BufReader::new(io::stdin()), plugin.clone(), &catalog).await?;

        Ok(())
    } else if let Some(cat) = args.catalog {
        info!("Generating catalog");

        let catalog = configure_catalog(&cat, plugin.clone()).await?;

        let json = serde_json::to_string(&catalog)?;

        debug!("{}", &json);

        print!("{}", &json);

        Ok(())
    } else {
        Err(Error::Anyhow(anyhow!("No option specified to execute.")))
    }
}

#[cfg(test)]
mod tests {
    use crate::SqlDestinationPlugin;
    use airbyte_protocol::message::{AirbyteMessage, AirbyteStateMessage};
    use anyhow::{anyhow, Error, Ok};
    use destination_iceberg::catalog::configure_catalog;
    use destination_iceberg::ingest::ingest;
    use destination_iceberg::plugin::DestinationPlugin;
    use destination_iceberg::state::generate_state;
    use iceberg_rust::catalog::identifier::Identifier;
    use iceberg_rust::catalog::tabular::Tabular;
    use serde_json::json;

    use std::fs::{self, File};
    use std::io::{BufReader, Write};
    use std::sync::Arc;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_orders() -> Result<(), Error> {
        let tempdir = tempdir()?;

        let config_path = tempdir.path().join("config.json");

        let mut config_file = File::create(config_path.clone())?;

        config_file.write_all(
            r#"
            {
            "catalogUrl": "sqlite://",
            "catalogName": "public"
            }
        "#
            .as_bytes(),
        )?;

        let plugin =
            Arc::new(SqlDestinationPlugin::new(config_path.as_path().to_str().unwrap()).await?);

        let airbyte_catalog =
            configure_catalog("../testdata/inventory/discover.json", plugin.clone()).await?;

        let input = File::open("../testdata/inventory/input1.txt")?;

        ingest(&mut BufReader::new(input), plugin.clone(), &airbyte_catalog).await?;

        let catalog = plugin.catalog().await?;

        let orders_table = if let Tabular::Table(table) = catalog
            .clone()
            .load_tabular(&Identifier::parse("inventory.orders")?)
            .await?
        {
            Ok(table)
        } else {
            Err(anyhow!("Not a table"))
        }?;

        let manifests = orders_table.manifests(None, None).await?;

        assert_eq!(manifests[0].added_rows_count.unwrap(), 2);

        let shared_state = orders_table
            .metadata()
            .properties
            .get("airbyte.shared_state")
            .expect("Failed to get bookmark");

        assert_eq!(
            shared_state,
            r#"{"state":{"[\"postgres\",{\"server\":\"postgres\"}]":"{\"transaction_id\":null,\"lsn\":37270192,\"txId\":780,\"ts_usec\":1720250537115587}"}}"#
        );

        let stream_state = orders_table
            .metadata()
            .properties
            .get("airbyte.stream_state")
            .expect("Failed to get bookmark");

        assert_eq!(
            stream_state,
            r#"{"cursor_field":[],"stream_name":"orders","stream_namespace":"inventory"}"#
        );

        let catalog_json = fs::read_to_string("../testdata/inventory/discover.json")?;

        let AirbyteMessage::Catalog {
            catalog: configured_airbyte_catalog,
        } = serde_json::from_str(&catalog_json)?
        else {
            return Err(anyhow!("No catalog in catalog file."));
        };

        let state = generate_state(plugin.clone(), &configured_airbyte_catalog).await?;

        let AirbyteStateMessage::Global {
            global,
            destination_stats: _,
            source_stats: _,
        } = state
        else {
            return Err(anyhow!("No global state"));
        };

        assert_eq!(
            global.shared_state.unwrap(),
            json!({"state": {r#"["postgres",{"server":"postgres"}]"#:r#"{"transaction_id":null,"lsn":37270192,"txId":780,"ts_usec":1720250537115587}"#}})
        );

        let products_table = if let Tabular::Table(table) = catalog
            .clone()
            .load_tabular(&Identifier::parse("inventory.products")?)
            .await?
        {
            Ok(table)
        } else {
            Err(anyhow!("Not a table"))
        }?;

        let manifests = products_table.manifests(None, None).await?;

        assert_eq!(manifests[0].added_rows_count.unwrap(), 5);

        let products_shared_state = products_table
            .metadata()
            .properties
            .get("airbyte.shared_state")
            .expect("Failed to get bookmark");

        assert_eq!(
            products_shared_state,
            r#"{"state":{"[\"postgres\",{\"server\":\"postgres\"}]":"{\"transaction_id\":null,\"lsn\":37270192,\"txId\":780,\"ts_usec\":1720250537115587}"}}"#
        );

        let input = File::open("../testdata/inventory/input2.txt")?;

        ingest(&mut BufReader::new(input), plugin.clone(), &airbyte_catalog).await?;

        let orders_table = if let Tabular::Table(table) = catalog
            .clone()
            .load_tabular(&Identifier::parse("inventory.orders")?)
            .await?
        {
            Ok(table)
        } else {
            Err(anyhow!("Not a table"))
        }?;

        let manifests = orders_table.manifests(None, None).await?;

        assert_eq!(manifests[0].added_rows_count.unwrap(), 4);

        Ok(())
    }
}
