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
use plugin::FileDestinationPlugin;
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

    let plugin = Arc::new(FileDestinationPlugin::new(&args.config).await?);

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
    use crate::FileDestinationPlugin;
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
            (r#"
            {
            "catalogUrl": ""#
                .to_string()
                + tempdir.path().to_str().unwrap()
                + r#""
            }
        "#)
            .as_bytes(),
        )?;

        let plugin =
            Arc::new(FileDestinationPlugin::new(config_path.as_path().to_str().unwrap()).await?);

        let airbyte_catalog =
            configure_catalog("../testdata/inventory/discover.json", plugin.clone()).await?;

        let input = File::open("../testdata/inventory/input1.txt")?;

        ingest(&mut BufReader::new(input), plugin.clone(), &airbyte_catalog).await?;

        let catalog = plugin.catalog().await?;

        let orders_table = if let Tabular::Table(table) = catalog
            .clone()
            .load_tabular(&Identifier::parse("inventory.orders", None)?)
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
            ref global,
            destination_stats: _,
            source_stats: _,
        } = state[0]
        else {
            return Err(anyhow!("No global state"));
        };

        assert_eq!(
            global.shared_state.as_ref().unwrap(),
            &json!({"state": {r#"["postgres",{"server":"postgres"}]"#:r#"{"transaction_id":null,"lsn":37270192,"txId":780,"ts_usec":1720250537115587}"#}})
        );

        let products_table = if let Tabular::Table(table) = catalog
            .clone()
            .load_tabular(&Identifier::parse("inventory.products", None)?)
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
            .load_tabular(&Identifier::parse("inventory.orders", None)?)
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

    #[tokio::test]
    async fn test_mysql() -> Result<(), Error> {
        let tempdir = tempdir()?;

        let config_path = tempdir.path().join("config.json");

        let mut config_file = File::create(config_path.clone())?;

        config_file.write_all(
            (r#"
            {
            "catalogUrl": ""#
                .to_string()
                + tempdir.path().to_str().unwrap()
                + r#""
            }
        "#)
            .as_bytes(),
        )?;

        let plugin =
            Arc::new(FileDestinationPlugin::new(config_path.as_path().to_str().unwrap()).await?);

        let airbyte_catalog =
            configure_catalog("../testdata/mysql/discover.json", plugin.clone()).await?;

        let input = File::open("../testdata/mysql/input1.txt")?;

        ingest(&mut BufReader::new(input), plugin.clone(), &airbyte_catalog).await?;

        let catalog = plugin.catalog().await?;

        let orders_table = if let Tabular::Table(table) = catalog
            .clone()
            .load_tabular(&Identifier::parse("inventory.orders", None)?)
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
            r#"{"state":{"mysql_cdc_offset":{"[\"inventory\",{\"server\":\"inventory\"}]":"{\"transaction_id\":null,\"ts_sec\":1722664430,\"file\":\"mysql-bin.000003\",\"pos\":4920}"},"mysql_db_history":"{\"source\":{\"server\":\"inventory\"},\"position\":{\"ts_sec\":1722664429,\"file\":\"mysql-bin.000003\",\"pos\":4920,\"snapshot\":true},\"ts_ms\":1722664430309,\"databaseName\":\"\",\"ddl\":\"SET character_set_server=utf8mb4, collation_server=utf8mb4_0900_ai_ci\",\"tableChanges\":[]}\n{\"source\":{\"server\":\"inventory\"},\"position\":{\"ts_sec\":1722664430,\"file\":\"mysql-bin.000003\",\"pos\":4920,\"snapshot\":true},\"ts_ms\":1722664430337,\"databaseName\":\"inventory\",\"ddl\":\"DROP TABLE IF EXISTS `inventory`.`addresses`\",\"tableChanges\":[]}\n{\"source\":{\"server\":\"inventory\"},\"position\":{\"ts_sec\":1722664430,\"file\":\"mysql-bin.000003\",\"pos\":4920,\"snapshot\":true},\"ts_ms\":1722664430339,\"databaseName\":\"inventory\",\"ddl\":\"DROP TABLE IF EXISTS `inventory`.`customers`\",\"tableChanges\":[]}\n{\"source\":{\"server\":\"inventory\"},\"position\":{\"ts_sec\":1722664430,\"file\":\"mysql-bin.000003\",\"pos\":4920,\"snapshot\":true},\"ts_ms\":1722664430341,\"databaseName\":\"inventory\",\"ddl\":\"DROP TABLE IF EXISTS `inventory`.`iceberg_namespace_properties`\",\"tableChanges\":[]}\n{\"source\":{\"server\":\"inventory\"},\"position\":{\"ts_sec\":1722664430,\"file\":\"mysql-bin.000003\",\"pos\":4920,\"snapshot\":true},\"ts_ms\":1722664430342,\"databaseName\":\"inventory\",\"ddl\":\"DROP TABLE IF EXISTS `inventory`.`iceberg_tables`\",\"tableChanges\":[]}\n{\"source\":{\"server\":\"inventory\"},\"position\":{\"ts_sec\":1722664430,\"file\":\"mysql-bin.000003\",\"pos\":4920,\"snapshot\":true},\"ts_ms\":1722664430343,\"databaseName\":\"inventory\",\"ddl\":\"DROP TABLE IF EXISTS `inventory`.`orders`\",\"tableChanges\":[]}\n{\"source\":{\"server\":\"inventory\"},\"position\":{\"ts_sec\":1722664430,\"file\":\"mysql-bin.000003\",\"pos\":4920,\"snapshot\":true},\"ts_ms\":1722664430344,\"databaseName\":\"inventory\",\"ddl\":\"DROP TABLE IF EXISTS `inventory`.`products`\",\"tableChanges\":[]}\n{\"source\":{\"server\":\"inventory\"},\"position\":{\"ts_sec\":1722664430,\"file\":\"mysql-bin.000003\",\"pos\":4920,\"snapshot\":true},\"ts_ms\":1722664430345,\"databaseName\":\"inventory\",\"ddl\":\"DROP TABLE IF EXISTS `inventory`.`products_on_hand`\",\"tableChanges\":[]}\n{\"source\":{\"server\":\"inventory\"},\"position\":{\"ts_sec\":1722664430,\"file\":\"mysql-bin.000003\",\"pos\":4920,\"snapshot\":true},\"ts_ms\":1722664430356,\"databaseName\":\"inventory\",\"ddl\":\"DROP DATABASE IF EXISTS `inventory`\",\"tableChanges\":[]}\n{\"source\":{\"server\":\"inventory\"},\"position\":{\"ts_sec\":1722664430,\"file\":\"mysql-bin.000003\",\"pos\":4920,\"snapshot\":true},\"ts_ms\":1722664430364,\"databaseName\":\"inventory\",\"ddl\":\"CREATE DATABASE `inventory` CHARSET utf8mb4 COLLATE utf8mb4_0900_ai_ci\",\"tableChanges\":[]}\n{\"source\":{\"server\":\"inventory\"},\"position\":{\"ts_sec\":1722664430,\"file\":\"mysql-bin.000003\",\"pos\":4920,\"snapshot\":true},\"ts_ms\":1722664430368,\"databaseName\":\"inventory\",\"ddl\":\"USE `inventory`\",\"tableChanges\":[]}\n{\"source\":{\"server\":\"inventory\"},\"position\":{\"ts_sec\":1722664430,\"file\":\"mysql-bin.000003\",\"pos\":4920,\"snapshot\":true},\"ts_ms\":1722664430461,\"databaseName\":\"inventory\",\"ddl\":\"CREATE TABLE `addresses` (\\n  `id` int NOT NULL AUTO_INCREMENT,\\n  `customer_id` int NOT NULL,\\n  `street` varchar(255) NOT NULL,\\n  `city` varchar(255) NOT NULL,\\n  `state` varchar(255) NOT NULL,\\n  `zip` varchar(255) NOT NULL,\\n  `type` enum('SHIPPING','BILLING','LIVING') NOT NULL,\\n  PRIMARY KEY (`id`),\\n  KEY `address_customer` (`customer_id`),\\n  CONSTRAINT `addresses_ibfk_1` FOREIGN KEY (`customer_id`) REFERENCES `customers` (`id`)\\n) ENGINE=InnoDB AUTO_INCREMENT=17 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci\",\"tableChanges\":[{\"type\":\"CREATE\",\"id\":\"\\\"inventory\\\".\\\"addresses\\\"\",\"table\":{\"defaultCharsetName\":\"utf8mb4\",\"primaryKeyColumnNames\":[\"id\"],\"columns\":[{\"name\":\"id\",\"jdbcType\":4,\"typeName\":\"INT\",\"typeExpression\":\"INT\",\"charsetName\":null,\"position\":1,\"optional\":false,\"autoIncremented\":true,\"generated\":true,\"comment\":null,\"hasDefaultValue\":false,\"enumValues\":[]},{\"name\":\"customer_id\",\"jdbcType\":4,\"typeName\":\"INT\",\"typeExpression\":\"INT\",\"charsetName\":null,\"position\":2,\"optional\":false,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":false,\"enumValues\":[]},{\"name\":\"street\",\"jdbcType\":12,\"typeName\":\"VARCHAR\",\"typeExpression\":\"VARCHAR\",\"charsetName\":\"utf8mb4\",\"length\":255,\"position\":3,\"optional\":false,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":false,\"enumValues\":[]},{\"name\":\"city\",\"jdbcType\":12,\"typeName\":\"VARCHAR\",\"typeExpression\":\"VARCHAR\",\"charsetName\":\"utf8mb4\",\"length\":255,\"position\":4,\"optional\":false,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":false,\"enumValues\":[]},{\"name\":\"state\",\"jdbcType\":12,\"typeName\":\"VARCHAR\",\"typeExpression\":\"VARCHAR\",\"charsetName\":\"utf8mb4\",\"length\":255,\"position\":5,\"optional\":false,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":false,\"enumValues\":[]},{\"name\":\"zip\",\"jdbcType\":12,\"typeName\":\"VARCHAR\",\"typeExpression\":\"VARCHAR\",\"charsetName\":\"utf8mb4\",\"length\":255,\"position\":6,\"optional\":false,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":false,\"enumValues\":[]},{\"name\":\"type\",\"jdbcType\":1,\"typeName\":\"ENUM\",\"typeExpression\":\"ENUM\",\"charsetName\":\"utf8mb4\",\"length\":1,\"position\":7,\"optional\":false,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":false,\"enumValues\":[\"'SHIPPING'\",\"'BILLING'\",\"'LIVING'\"]}],\"attributes\":[]},\"comment\":null}]}\n{\"source\":{\"server\":\"inventory\"},\"position\":{\"ts_sec\":1722664430,\"file\":\"mysql-bin.000003\",\"pos\":4920,\"snapshot\":true},\"ts_ms\":1722664430480,\"databaseName\":\"inventory\",\"ddl\":\"CREATE TABLE `customers` (\\n  `id` int NOT NULL AUTO_INCREMENT,\\n  `first_name` varchar(255) NOT NULL,\\n  `last_name` varchar(255) NOT NULL,\\n  `email` varchar(255) NOT NULL,\\n  PRIMARY KEY (`id`),\\n  UNIQUE KEY `email` (`email`)\\n) ENGINE=InnoDB AUTO_INCREMENT=1005 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci\",\"tableChanges\":[{\"type\":\"CREATE\",\"id\":\"\\\"inventory\\\".\\\"customers\\\"\",\"table\":{\"defaultCharsetName\":\"utf8mb4\",\"primaryKeyColumnNames\":[\"id\"],\"columns\":[{\"name\":\"id\",\"jdbcType\":4,\"typeName\":\"INT\",\"typeExpression\":\"INT\",\"charsetName\":null,\"position\":1,\"optional\":false,\"autoIncremented\":true,\"generated\":true,\"comment\":null,\"hasDefaultValue\":false,\"enumValues\":[]},{\"name\":\"first_name\",\"jdbcType\":12,\"typeName\":\"VARCHAR\",\"typeExpression\":\"VARCHAR\",\"charsetName\":\"utf8mb4\",\"length\":255,\"position\":2,\"optional\":false,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":false,\"enumValues\":[]},{\"name\":\"last_name\",\"jdbcType\":12,\"typeName\":\"VARCHAR\",\"typeExpression\":\"VARCHAR\",\"charsetName\":\"utf8mb4\",\"length\":255,\"position\":3,\"optional\":false,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":false,\"enumValues\":[]},{\"name\":\"email\",\"jdbcType\":12,\"typeName\":\"VARCHAR\",\"typeExpression\":\"VARCHAR\",\"charsetName\":\"utf8mb4\",\"length\":255,\"position\":4,\"optional\":false,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":false,\"enumValues\":[]}],\"attributes\":[]},\"comment\":null}]}\n{\"source\":{\"server\":\"inventory\"},\"position\":{\"ts_sec\":1722664430,\"file\":\"mysql-bin.000003\",\"pos\":4920,\"snapshot\":true},\"ts_ms\":1722664430509,\"databaseName\":\"inventory\",\"ddl\":\"CREATE TABLE `iceberg_namespace_properties` (\\n  `catalog_name` varchar(255) NOT NULL,\\n  `namespace` varchar(255) NOT NULL,\\n  `property_key` varchar(255) NOT NULL,\\n  `property_value` varchar(255) DEFAULT NULL,\\n  PRIMARY KEY (`catalog_name`,`namespace`,`property_key`)\\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci\",\"tableChanges\":[{\"type\":\"CREATE\",\"id\":\"\\\"inventory\\\".\\\"iceberg_namespace_properties\\\"\",\"table\":{\"defaultCharsetName\":\"utf8mb4\",\"primaryKeyColumnNames\":[\"catalog_name\",\"namespace\",\"property_key\"],\"columns\":[{\"name\":\"catalog_name\",\"jdbcType\":12,\"typeName\":\"VARCHAR\",\"typeExpression\":\"VARCHAR\",\"charsetName\":\"utf8mb4\",\"length\":255,\"position\":1,\"optional\":false,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":false,\"enumValues\":[]},{\"name\":\"namespace\",\"jdbcType\":12,\"typeName\":\"VARCHAR\",\"typeExpression\":\"VARCHAR\",\"charsetName\":\"utf8mb4\",\"length\":255,\"position\":2,\"optional\":false,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":false,\"enumValues\":[]},{\"name\":\"property_key\",\"jdbcType\":12,\"typeName\":\"VARCHAR\",\"typeExpression\":\"VARCHAR\",\"charsetName\":\"utf8mb4\",\"length\":255,\"position\":3,\"optional\":false,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":false,\"enumValues\":[]},{\"name\":\"property_value\",\"jdbcType\":12,\"typeName\":\"VARCHAR\",\"typeExpression\":\"VARCHAR\",\"charsetName\":\"utf8mb4\",\"length\":255,\"position\":4,\"optional\":true,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":true,\"enumValues\":[]}],\"attributes\":[]},\"comment\":null}]}\n{\"source\":{\"server\":\"inventory\"},\"position\":{\"ts_sec\":1722664430,\"file\":\"mysql-bin.000003\",\"pos\":4920,\"snapshot\":true},\"ts_ms\":1722664430518,\"databaseName\":\"inventory\",\"ddl\":\"CREATE TABLE `iceberg_tables` (\\n  `catalog_name` varchar(255) NOT NULL,\\n  `table_namespace` varchar(255) NOT NULL,\\n  `table_name` varchar(255) NOT NULL,\\n  `metadata_location` varchar(255) NOT NULL,\\n  `previous_metadata_location` varchar(255) DEFAULT NULL,\\n  PRIMARY KEY (`catalog_name`,`table_namespace`,`table_name`)\\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci\",\"tableChanges\":[{\"type\":\"CREATE\",\"id\":\"\\\"inventory\\\".\\\"iceberg_tables\\\"\",\"table\":{\"defaultCharsetName\":\"utf8mb4\",\"primaryKeyColumnNames\":[\"catalog_name\",\"table_namespace\",\"table_name\"],\"columns\":[{\"name\":\"catalog_name\",\"jdbcType\":12,\"typeName\":\"VARCHAR\",\"typeExpression\":\"VARCHAR\",\"charsetName\":\"utf8mb4\",\"length\":255,\"position\":1,\"optional\":false,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":false,\"enumValues\":[]},{\"name\":\"table_namespace\",\"jdbcType\":12,\"typeName\":\"VARCHAR\",\"typeExpression\":\"VARCHAR\",\"charsetName\":\"utf8mb4\",\"length\":255,\"position\":2,\"optional\":false,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":false,\"enumValues\":[]},{\"name\":\"table_name\",\"jdbcType\":12,\"typeName\":\"VARCHAR\",\"typeExpression\":\"VARCHAR\",\"charsetName\":\"utf8mb4\",\"length\":255,\"position\":3,\"optional\":false,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":false,\"enumValues\":[]},{\"name\":\"metadata_location\",\"jdbcType\":12,\"typeName\":\"VARCHAR\",\"typeExpression\":\"VARCHAR\",\"charsetName\":\"utf8mb4\",\"length\":255,\"position\":4,\"optional\":false,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":false,\"enumValues\":[]},{\"name\":\"previous_metadata_location\",\"jdbcType\":12,\"typeName\":\"VARCHAR\",\"typeExpression\":\"VARCHAR\",\"charsetName\":\"utf8mb4\",\"length\":255,\"position\":5,\"optional\":true,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":true,\"enumValues\":[]}],\"attributes\":[]},\"comment\":null}]}\n{\"source\":{\"server\":\"inventory\"},\"position\":{\"ts_sec\":1722664430,\"file\":\"mysql-bin.000003\",\"pos\":4920,\"snapshot\":true},\"ts_ms\":1722664430529,\"databaseName\":\"inventory\",\"ddl\":\"CREATE TABLE `orders` (\\n  `order_number` int NOT NULL AUTO_INCREMENT,\\n  `order_date` date NOT NULL,\\n  `purchaser` int NOT NULL,\\n  `quantity` int NOT NULL,\\n  `product_id` int NOT NULL,\\n  PRIMARY KEY (`order_number`),\\n  KEY `order_customer` (`purchaser`),\\n  KEY `ordered_product` (`product_id`),\\n  CONSTRAINT `orders_ibfk_1` FOREIGN KEY (`purchaser`) REFERENCES `customers` (`id`),\\n  CONSTRAINT `orders_ibfk_2` FOREIGN KEY (`product_id`) REFERENCES `products` (`id`)\\n) ENGINE=InnoDB AUTO_INCREMENT=10005 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci\",\"tableChanges\":[{\"type\":\"CREATE\",\"id\":\"\\\"inventory\\\".\\\"orders\\\"\",\"table\":{\"defaultCharsetName\":\"utf8mb4\",\"primaryKeyColumnNames\":[\"order_number\"],\"columns\":[{\"name\":\"order_number\",\"jdbcType\":4,\"typeName\":\"INT\",\"typeExpression\":\"INT\",\"charsetName\":null,\"position\":1,\"optional\":false,\"autoIncremented\":true,\"generated\":true,\"comment\":null,\"hasDefaultValue\":false,\"enumValues\":[]},{\"name\":\"order_date\",\"jdbcType\":91,\"typeName\":\"DATE\",\"typeExpression\":\"DATE\",\"charsetName\":null,\"position\":2,\"optional\":false,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":false,\"enumValues\":[]},{\"name\":\"purchaser\",\"jdbcType\":4,\"typeName\":\"INT\",\"typeExpression\":\"INT\",\"charsetName\":null,\"position\":3,\"optional\":false,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":false,\"enumValues\":[]},{\"name\":\"quantity\",\"jdbcType\":4,\"typeName\":\"INT\",\"typeExpression\":\"INT\",\"charsetName\":null,\"position\":4,\"optional\":false,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":false,\"enumValues\":[]},{\"name\":\"product_id\",\"jdbcType\":4,\"typeName\":\"INT\",\"typeExpression\":\"INT\",\"charsetName\":null,\"position\":5,\"optional\":false,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":false,\"enumValues\":[]}],\"attributes\":[]},\"comment\":null}]}\n{\"source\":{\"server\":\"inventory\"},\"position\":{\"ts_sec\":1722664430,\"file\":\"mysql-bin.000003\",\"pos\":4920,\"snapshot\":true},\"ts_ms\":1722664430544,\"databaseName\":\"inventory\",\"ddl\":\"CREATE TABLE `products` (\\n  `id` int NOT NULL AUTO_INCREMENT,\\n  `name` varchar(255) NOT NULL,\\n  `description` varchar(512) DEFAULT NULL,\\n  `weight` float DEFAULT NULL,\\n  PRIMARY KEY (`id`)\\n) ENGINE=InnoDB AUTO_INCREMENT=110 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci\",\"tableChanges\":[{\"type\":\"CREATE\",\"id\":\"\\\"inventory\\\".\\\"products\\\"\",\"table\":{\"defaultCharsetName\":\"utf8mb4\",\"primaryKeyColumnNames\":[\"id\"],\"columns\":[{\"name\":\"id\",\"jdbcType\":4,\"typeName\":\"INT\",\"typeExpression\":\"INT\",\"charsetName\":null,\"position\":1,\"optional\":false,\"autoIncremented\":true,\"generated\":true,\"comment\":null,\"hasDefaultValue\":false,\"enumValues\":[]},{\"name\":\"name\",\"jdbcType\":12,\"typeName\":\"VARCHAR\",\"typeExpression\":\"VARCHAR\",\"charsetName\":\"utf8mb4\",\"length\":255,\"position\":2,\"optional\":false,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":false,\"enumValues\":[]},{\"name\":\"description\",\"jdbcType\":12,\"typeName\":\"VARCHAR\",\"typeExpression\":\"VARCHAR\",\"charsetName\":\"utf8mb4\",\"length\":512,\"position\":3,\"optional\":true,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":true,\"enumValues\":[]},{\"name\":\"weight\",\"jdbcType\":6,\"typeName\":\"FLOAT\",\"typeExpression\":\"FLOAT\",\"charsetName\":null,\"position\":4,\"optional\":true,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":true,\"enumValues\":[]}],\"attributes\":[]},\"comment\":null}]}\n{\"source\":{\"server\":\"inventory\"},\"position\":{\"ts_sec\":1722664430,\"file\":\"mysql-bin.000003\",\"pos\":4920,\"snapshot\":true},\"ts_ms\":1722664430549,\"databaseName\":\"inventory\",\"ddl\":\"CREATE TABLE `products_on_hand` (\\n  `product_id` int NOT NULL,\\n  `quantity` int NOT NULL,\\n  PRIMARY KEY (`product_id`),\\n  CONSTRAINT `products_on_hand_ibfk_1` FOREIGN KEY (`product_id`) REFERENCES `products` (`id`)\\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci\",\"tableChanges\":[{\"type\":\"CREATE\",\"id\":\"\\\"inventory\\\".\\\"products_on_hand\\\"\",\"table\":{\"defaultCharsetName\":\"utf8mb4\",\"primaryKeyColumnNames\":[\"product_id\"],\"columns\":[{\"name\":\"product_id\",\"jdbcType\":4,\"typeName\":\"INT\",\"typeExpression\":\"INT\",\"charsetName\":null,\"position\":1,\"optional\":false,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":false,\"enumValues\":[]},{\"name\":\"quantity\",\"jdbcType\":4,\"typeName\":\"INT\",\"typeExpression\":\"INT\",\"charsetName\":null,\"position\":2,\"optional\":false,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":false,\"enumValues\":[]}],\"attributes\":[]},\"comment\":null}]}\n"}}"#
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

        let catalog_json = fs::read_to_string("../testdata/mysql/discover.json")?;

        let AirbyteMessage::Catalog {
            catalog: configured_airbyte_catalog,
        } = serde_json::from_str(&catalog_json)?
        else {
            return Err(anyhow!("No catalog in catalog file."));
        };

        let state = generate_state(plugin.clone(), &configured_airbyte_catalog).await?;

        let AirbyteStateMessage::Global {
            ref global,
            destination_stats: _,
            source_stats: _,
        } = state[0]
        else {
            return Err(anyhow!("No global state"));
        };

        assert_eq!(
            global.shared_state.as_ref().unwrap(),
            &json!({"state": {"mysql_cdc_offset":{r#"["inventory",{"server":"inventory"}]"#:r#"{"transaction_id":null,"ts_sec":1722664430,"file":"mysql-bin.000003","pos":4920}"#}, "mysql_db_history": "{\"source\":{\"server\":\"inventory\"},\"position\":{\"ts_sec\":1722664429,\"file\":\"mysql-bin.000003\",\"pos\":4920,\"snapshot\":true},\"ts_ms\":1722664430309,\"databaseName\":\"\",\"ddl\":\"SET character_set_server=utf8mb4, collation_server=utf8mb4_0900_ai_ci\",\"tableChanges\":[]}\n{\"source\":{\"server\":\"inventory\"},\"position\":{\"ts_sec\":1722664430,\"file\":\"mysql-bin.000003\",\"pos\":4920,\"snapshot\":true},\"ts_ms\":1722664430337,\"databaseName\":\"inventory\",\"ddl\":\"DROP TABLE IF EXISTS `inventory`.`addresses`\",\"tableChanges\":[]}\n{\"source\":{\"server\":\"inventory\"},\"position\":{\"ts_sec\":1722664430,\"file\":\"mysql-bin.000003\",\"pos\":4920,\"snapshot\":true},\"ts_ms\":1722664430339,\"databaseName\":\"inventory\",\"ddl\":\"DROP TABLE IF EXISTS `inventory`.`customers`\",\"tableChanges\":[]}\n{\"source\":{\"server\":\"inventory\"},\"position\":{\"ts_sec\":1722664430,\"file\":\"mysql-bin.000003\",\"pos\":4920,\"snapshot\":true},\"ts_ms\":1722664430341,\"databaseName\":\"inventory\",\"ddl\":\"DROP TABLE IF EXISTS `inventory`.`iceberg_namespace_properties`\",\"tableChanges\":[]}\n{\"source\":{\"server\":\"inventory\"},\"position\":{\"ts_sec\":1722664430,\"file\":\"mysql-bin.000003\",\"pos\":4920,\"snapshot\":true},\"ts_ms\":1722664430342,\"databaseName\":\"inventory\",\"ddl\":\"DROP TABLE IF EXISTS `inventory`.`iceberg_tables`\",\"tableChanges\":[]}\n{\"source\":{\"server\":\"inventory\"},\"position\":{\"ts_sec\":1722664430,\"file\":\"mysql-bin.000003\",\"pos\":4920,\"snapshot\":true},\"ts_ms\":1722664430343,\"databaseName\":\"inventory\",\"ddl\":\"DROP TABLE IF EXISTS `inventory`.`orders`\",\"tableChanges\":[]}\n{\"source\":{\"server\":\"inventory\"},\"position\":{\"ts_sec\":1722664430,\"file\":\"mysql-bin.000003\",\"pos\":4920,\"snapshot\":true},\"ts_ms\":1722664430344,\"databaseName\":\"inventory\",\"ddl\":\"DROP TABLE IF EXISTS `inventory`.`products`\",\"tableChanges\":[]}\n{\"source\":{\"server\":\"inventory\"},\"position\":{\"ts_sec\":1722664430,\"file\":\"mysql-bin.000003\",\"pos\":4920,\"snapshot\":true},\"ts_ms\":1722664430345,\"databaseName\":\"inventory\",\"ddl\":\"DROP TABLE IF EXISTS `inventory`.`products_on_hand`\",\"tableChanges\":[]}\n{\"source\":{\"server\":\"inventory\"},\"position\":{\"ts_sec\":1722664430,\"file\":\"mysql-bin.000003\",\"pos\":4920,\"snapshot\":true},\"ts_ms\":1722664430356,\"databaseName\":\"inventory\",\"ddl\":\"DROP DATABASE IF EXISTS `inventory`\",\"tableChanges\":[]}\n{\"source\":{\"server\":\"inventory\"},\"position\":{\"ts_sec\":1722664430,\"file\":\"mysql-bin.000003\",\"pos\":4920,\"snapshot\":true},\"ts_ms\":1722664430364,\"databaseName\":\"inventory\",\"ddl\":\"CREATE DATABASE `inventory` CHARSET utf8mb4 COLLATE utf8mb4_0900_ai_ci\",\"tableChanges\":[]}\n{\"source\":{\"server\":\"inventory\"},\"position\":{\"ts_sec\":1722664430,\"file\":\"mysql-bin.000003\",\"pos\":4920,\"snapshot\":true},\"ts_ms\":1722664430368,\"databaseName\":\"inventory\",\"ddl\":\"USE `inventory`\",\"tableChanges\":[]}\n{\"source\":{\"server\":\"inventory\"},\"position\":{\"ts_sec\":1722664430,\"file\":\"mysql-bin.000003\",\"pos\":4920,\"snapshot\":true},\"ts_ms\":1722664430461,\"databaseName\":\"inventory\",\"ddl\":\"CREATE TABLE `addresses` (\\n  `id` int NOT NULL AUTO_INCREMENT,\\n  `customer_id` int NOT NULL,\\n  `street` varchar(255) NOT NULL,\\n  `city` varchar(255) NOT NULL,\\n  `state` varchar(255) NOT NULL,\\n  `zip` varchar(255) NOT NULL,\\n  `type` enum('SHIPPING','BILLING','LIVING') NOT NULL,\\n  PRIMARY KEY (`id`),\\n  KEY `address_customer` (`customer_id`),\\n  CONSTRAINT `addresses_ibfk_1` FOREIGN KEY (`customer_id`) REFERENCES `customers` (`id`)\\n) ENGINE=InnoDB AUTO_INCREMENT=17 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci\",\"tableChanges\":[{\"type\":\"CREATE\",\"id\":\"\\\"inventory\\\".\\\"addresses\\\"\",\"table\":{\"defaultCharsetName\":\"utf8mb4\",\"primaryKeyColumnNames\":[\"id\"],\"columns\":[{\"name\":\"id\",\"jdbcType\":4,\"typeName\":\"INT\",\"typeExpression\":\"INT\",\"charsetName\":null,\"position\":1,\"optional\":false,\"autoIncremented\":true,\"generated\":true,\"comment\":null,\"hasDefaultValue\":false,\"enumValues\":[]},{\"name\":\"customer_id\",\"jdbcType\":4,\"typeName\":\"INT\",\"typeExpression\":\"INT\",\"charsetName\":null,\"position\":2,\"optional\":false,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":false,\"enumValues\":[]},{\"name\":\"street\",\"jdbcType\":12,\"typeName\":\"VARCHAR\",\"typeExpression\":\"VARCHAR\",\"charsetName\":\"utf8mb4\",\"length\":255,\"position\":3,\"optional\":false,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":false,\"enumValues\":[]},{\"name\":\"city\",\"jdbcType\":12,\"typeName\":\"VARCHAR\",\"typeExpression\":\"VARCHAR\",\"charsetName\":\"utf8mb4\",\"length\":255,\"position\":4,\"optional\":false,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":false,\"enumValues\":[]},{\"name\":\"state\",\"jdbcType\":12,\"typeName\":\"VARCHAR\",\"typeExpression\":\"VARCHAR\",\"charsetName\":\"utf8mb4\",\"length\":255,\"position\":5,\"optional\":false,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":false,\"enumValues\":[]},{\"name\":\"zip\",\"jdbcType\":12,\"typeName\":\"VARCHAR\",\"typeExpression\":\"VARCHAR\",\"charsetName\":\"utf8mb4\",\"length\":255,\"position\":6,\"optional\":false,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":false,\"enumValues\":[]},{\"name\":\"type\",\"jdbcType\":1,\"typeName\":\"ENUM\",\"typeExpression\":\"ENUM\",\"charsetName\":\"utf8mb4\",\"length\":1,\"position\":7,\"optional\":false,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":false,\"enumValues\":[\"'SHIPPING'\",\"'BILLING'\",\"'LIVING'\"]}],\"attributes\":[]},\"comment\":null}]}\n{\"source\":{\"server\":\"inventory\"},\"position\":{\"ts_sec\":1722664430,\"file\":\"mysql-bin.000003\",\"pos\":4920,\"snapshot\":true},\"ts_ms\":1722664430480,\"databaseName\":\"inventory\",\"ddl\":\"CREATE TABLE `customers` (\\n  `id` int NOT NULL AUTO_INCREMENT,\\n  `first_name` varchar(255) NOT NULL,\\n  `last_name` varchar(255) NOT NULL,\\n  `email` varchar(255) NOT NULL,\\n  PRIMARY KEY (`id`),\\n  UNIQUE KEY `email` (`email`)\\n) ENGINE=InnoDB AUTO_INCREMENT=1005 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci\",\"tableChanges\":[{\"type\":\"CREATE\",\"id\":\"\\\"inventory\\\".\\\"customers\\\"\",\"table\":{\"defaultCharsetName\":\"utf8mb4\",\"primaryKeyColumnNames\":[\"id\"],\"columns\":[{\"name\":\"id\",\"jdbcType\":4,\"typeName\":\"INT\",\"typeExpression\":\"INT\",\"charsetName\":null,\"position\":1,\"optional\":false,\"autoIncremented\":true,\"generated\":true,\"comment\":null,\"hasDefaultValue\":false,\"enumValues\":[]},{\"name\":\"first_name\",\"jdbcType\":12,\"typeName\":\"VARCHAR\",\"typeExpression\":\"VARCHAR\",\"charsetName\":\"utf8mb4\",\"length\":255,\"position\":2,\"optional\":false,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":false,\"enumValues\":[]},{\"name\":\"last_name\",\"jdbcType\":12,\"typeName\":\"VARCHAR\",\"typeExpression\":\"VARCHAR\",\"charsetName\":\"utf8mb4\",\"length\":255,\"position\":3,\"optional\":false,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":false,\"enumValues\":[]},{\"name\":\"email\",\"jdbcType\":12,\"typeName\":\"VARCHAR\",\"typeExpression\":\"VARCHAR\",\"charsetName\":\"utf8mb4\",\"length\":255,\"position\":4,\"optional\":false,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":false,\"enumValues\":[]}],\"attributes\":[]},\"comment\":null}]}\n{\"source\":{\"server\":\"inventory\"},\"position\":{\"ts_sec\":1722664430,\"file\":\"mysql-bin.000003\",\"pos\":4920,\"snapshot\":true},\"ts_ms\":1722664430509,\"databaseName\":\"inventory\",\"ddl\":\"CREATE TABLE `iceberg_namespace_properties` (\\n  `catalog_name` varchar(255) NOT NULL,\\n  `namespace` varchar(255) NOT NULL,\\n  `property_key` varchar(255) NOT NULL,\\n  `property_value` varchar(255) DEFAULT NULL,\\n  PRIMARY KEY (`catalog_name`,`namespace`,`property_key`)\\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci\",\"tableChanges\":[{\"type\":\"CREATE\",\"id\":\"\\\"inventory\\\".\\\"iceberg_namespace_properties\\\"\",\"table\":{\"defaultCharsetName\":\"utf8mb4\",\"primaryKeyColumnNames\":[\"catalog_name\",\"namespace\",\"property_key\"],\"columns\":[{\"name\":\"catalog_name\",\"jdbcType\":12,\"typeName\":\"VARCHAR\",\"typeExpression\":\"VARCHAR\",\"charsetName\":\"utf8mb4\",\"length\":255,\"position\":1,\"optional\":false,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":false,\"enumValues\":[]},{\"name\":\"namespace\",\"jdbcType\":12,\"typeName\":\"VARCHAR\",\"typeExpression\":\"VARCHAR\",\"charsetName\":\"utf8mb4\",\"length\":255,\"position\":2,\"optional\":false,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":false,\"enumValues\":[]},{\"name\":\"property_key\",\"jdbcType\":12,\"typeName\":\"VARCHAR\",\"typeExpression\":\"VARCHAR\",\"charsetName\":\"utf8mb4\",\"length\":255,\"position\":3,\"optional\":false,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":false,\"enumValues\":[]},{\"name\":\"property_value\",\"jdbcType\":12,\"typeName\":\"VARCHAR\",\"typeExpression\":\"VARCHAR\",\"charsetName\":\"utf8mb4\",\"length\":255,\"position\":4,\"optional\":true,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":true,\"enumValues\":[]}],\"attributes\":[]},\"comment\":null}]}\n{\"source\":{\"server\":\"inventory\"},\"position\":{\"ts_sec\":1722664430,\"file\":\"mysql-bin.000003\",\"pos\":4920,\"snapshot\":true},\"ts_ms\":1722664430518,\"databaseName\":\"inventory\",\"ddl\":\"CREATE TABLE `iceberg_tables` (\\n  `catalog_name` varchar(255) NOT NULL,\\n  `table_namespace` varchar(255) NOT NULL,\\n  `table_name` varchar(255) NOT NULL,\\n  `metadata_location` varchar(255) NOT NULL,\\n  `previous_metadata_location` varchar(255) DEFAULT NULL,\\n  PRIMARY KEY (`catalog_name`,`table_namespace`,`table_name`)\\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci\",\"tableChanges\":[{\"type\":\"CREATE\",\"id\":\"\\\"inventory\\\".\\\"iceberg_tables\\\"\",\"table\":{\"defaultCharsetName\":\"utf8mb4\",\"primaryKeyColumnNames\":[\"catalog_name\",\"table_namespace\",\"table_name\"],\"columns\":[{\"name\":\"catalog_name\",\"jdbcType\":12,\"typeName\":\"VARCHAR\",\"typeExpression\":\"VARCHAR\",\"charsetName\":\"utf8mb4\",\"length\":255,\"position\":1,\"optional\":false,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":false,\"enumValues\":[]},{\"name\":\"table_namespace\",\"jdbcType\":12,\"typeName\":\"VARCHAR\",\"typeExpression\":\"VARCHAR\",\"charsetName\":\"utf8mb4\",\"length\":255,\"position\":2,\"optional\":false,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":false,\"enumValues\":[]},{\"name\":\"table_name\",\"jdbcType\":12,\"typeName\":\"VARCHAR\",\"typeExpression\":\"VARCHAR\",\"charsetName\":\"utf8mb4\",\"length\":255,\"position\":3,\"optional\":false,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":false,\"enumValues\":[]},{\"name\":\"metadata_location\",\"jdbcType\":12,\"typeName\":\"VARCHAR\",\"typeExpression\":\"VARCHAR\",\"charsetName\":\"utf8mb4\",\"length\":255,\"position\":4,\"optional\":false,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":false,\"enumValues\":[]},{\"name\":\"previous_metadata_location\",\"jdbcType\":12,\"typeName\":\"VARCHAR\",\"typeExpression\":\"VARCHAR\",\"charsetName\":\"utf8mb4\",\"length\":255,\"position\":5,\"optional\":true,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":true,\"enumValues\":[]}],\"attributes\":[]},\"comment\":null}]}\n{\"source\":{\"server\":\"inventory\"},\"position\":{\"ts_sec\":1722664430,\"file\":\"mysql-bin.000003\",\"pos\":4920,\"snapshot\":true},\"ts_ms\":1722664430529,\"databaseName\":\"inventory\",\"ddl\":\"CREATE TABLE `orders` (\\n  `order_number` int NOT NULL AUTO_INCREMENT,\\n  `order_date` date NOT NULL,\\n  `purchaser` int NOT NULL,\\n  `quantity` int NOT NULL,\\n  `product_id` int NOT NULL,\\n  PRIMARY KEY (`order_number`),\\n  KEY `order_customer` (`purchaser`),\\n  KEY `ordered_product` (`product_id`),\\n  CONSTRAINT `orders_ibfk_1` FOREIGN KEY (`purchaser`) REFERENCES `customers` (`id`),\\n  CONSTRAINT `orders_ibfk_2` FOREIGN KEY (`product_id`) REFERENCES `products` (`id`)\\n) ENGINE=InnoDB AUTO_INCREMENT=10005 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci\",\"tableChanges\":[{\"type\":\"CREATE\",\"id\":\"\\\"inventory\\\".\\\"orders\\\"\",\"table\":{\"defaultCharsetName\":\"utf8mb4\",\"primaryKeyColumnNames\":[\"order_number\"],\"columns\":[{\"name\":\"order_number\",\"jdbcType\":4,\"typeName\":\"INT\",\"typeExpression\":\"INT\",\"charsetName\":null,\"position\":1,\"optional\":false,\"autoIncremented\":true,\"generated\":true,\"comment\":null,\"hasDefaultValue\":false,\"enumValues\":[]},{\"name\":\"order_date\",\"jdbcType\":91,\"typeName\":\"DATE\",\"typeExpression\":\"DATE\",\"charsetName\":null,\"position\":2,\"optional\":false,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":false,\"enumValues\":[]},{\"name\":\"purchaser\",\"jdbcType\":4,\"typeName\":\"INT\",\"typeExpression\":\"INT\",\"charsetName\":null,\"position\":3,\"optional\":false,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":false,\"enumValues\":[]},{\"name\":\"quantity\",\"jdbcType\":4,\"typeName\":\"INT\",\"typeExpression\":\"INT\",\"charsetName\":null,\"position\":4,\"optional\":false,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":false,\"enumValues\":[]},{\"name\":\"product_id\",\"jdbcType\":4,\"typeName\":\"INT\",\"typeExpression\":\"INT\",\"charsetName\":null,\"position\":5,\"optional\":false,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":false,\"enumValues\":[]}],\"attributes\":[]},\"comment\":null}]}\n{\"source\":{\"server\":\"inventory\"},\"position\":{\"ts_sec\":1722664430,\"file\":\"mysql-bin.000003\",\"pos\":4920,\"snapshot\":true},\"ts_ms\":1722664430544,\"databaseName\":\"inventory\",\"ddl\":\"CREATE TABLE `products` (\\n  `id` int NOT NULL AUTO_INCREMENT,\\n  `name` varchar(255) NOT NULL,\\n  `description` varchar(512) DEFAULT NULL,\\n  `weight` float DEFAULT NULL,\\n  PRIMARY KEY (`id`)\\n) ENGINE=InnoDB AUTO_INCREMENT=110 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci\",\"tableChanges\":[{\"type\":\"CREATE\",\"id\":\"\\\"inventory\\\".\\\"products\\\"\",\"table\":{\"defaultCharsetName\":\"utf8mb4\",\"primaryKeyColumnNames\":[\"id\"],\"columns\":[{\"name\":\"id\",\"jdbcType\":4,\"typeName\":\"INT\",\"typeExpression\":\"INT\",\"charsetName\":null,\"position\":1,\"optional\":false,\"autoIncremented\":true,\"generated\":true,\"comment\":null,\"hasDefaultValue\":false,\"enumValues\":[]},{\"name\":\"name\",\"jdbcType\":12,\"typeName\":\"VARCHAR\",\"typeExpression\":\"VARCHAR\",\"charsetName\":\"utf8mb4\",\"length\":255,\"position\":2,\"optional\":false,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":false,\"enumValues\":[]},{\"name\":\"description\",\"jdbcType\":12,\"typeName\":\"VARCHAR\",\"typeExpression\":\"VARCHAR\",\"charsetName\":\"utf8mb4\",\"length\":512,\"position\":3,\"optional\":true,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":true,\"enumValues\":[]},{\"name\":\"weight\",\"jdbcType\":6,\"typeName\":\"FLOAT\",\"typeExpression\":\"FLOAT\",\"charsetName\":null,\"position\":4,\"optional\":true,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":true,\"enumValues\":[]}],\"attributes\":[]},\"comment\":null}]}\n{\"source\":{\"server\":\"inventory\"},\"position\":{\"ts_sec\":1722664430,\"file\":\"mysql-bin.000003\",\"pos\":4920,\"snapshot\":true},\"ts_ms\":1722664430549,\"databaseName\":\"inventory\",\"ddl\":\"CREATE TABLE `products_on_hand` (\\n  `product_id` int NOT NULL,\\n  `quantity` int NOT NULL,\\n  PRIMARY KEY (`product_id`),\\n  CONSTRAINT `products_on_hand_ibfk_1` FOREIGN KEY (`product_id`) REFERENCES `products` (`id`)\\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci\",\"tableChanges\":[{\"type\":\"CREATE\",\"id\":\"\\\"inventory\\\".\\\"products_on_hand\\\"\",\"table\":{\"defaultCharsetName\":\"utf8mb4\",\"primaryKeyColumnNames\":[\"product_id\"],\"columns\":[{\"name\":\"product_id\",\"jdbcType\":4,\"typeName\":\"INT\",\"typeExpression\":\"INT\",\"charsetName\":null,\"position\":1,\"optional\":false,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":false,\"enumValues\":[]},{\"name\":\"quantity\",\"jdbcType\":4,\"typeName\":\"INT\",\"typeExpression\":\"INT\",\"charsetName\":null,\"position\":2,\"optional\":false,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":false,\"enumValues\":[]}],\"attributes\":[]},\"comment\":null}]}\n"}})
        );

        let products_table = if let Tabular::Table(table) = catalog
            .clone()
            .load_tabular(&Identifier::parse("inventory.products", None)?)
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
            r#"{"state":{"mysql_cdc_offset":{"[\"inventory\",{\"server\":\"inventory\"}]":"{\"transaction_id\":null,\"ts_sec\":1722664430,\"file\":\"mysql-bin.000003\",\"pos\":4920}"},"mysql_db_history":"{\"source\":{\"server\":\"inventory\"},\"position\":{\"ts_sec\":1722664429,\"file\":\"mysql-bin.000003\",\"pos\":4920,\"snapshot\":true},\"ts_ms\":1722664430309,\"databaseName\":\"\",\"ddl\":\"SET character_set_server=utf8mb4, collation_server=utf8mb4_0900_ai_ci\",\"tableChanges\":[]}\n{\"source\":{\"server\":\"inventory\"},\"position\":{\"ts_sec\":1722664430,\"file\":\"mysql-bin.000003\",\"pos\":4920,\"snapshot\":true},\"ts_ms\":1722664430337,\"databaseName\":\"inventory\",\"ddl\":\"DROP TABLE IF EXISTS `inventory`.`addresses`\",\"tableChanges\":[]}\n{\"source\":{\"server\":\"inventory\"},\"position\":{\"ts_sec\":1722664430,\"file\":\"mysql-bin.000003\",\"pos\":4920,\"snapshot\":true},\"ts_ms\":1722664430339,\"databaseName\":\"inventory\",\"ddl\":\"DROP TABLE IF EXISTS `inventory`.`customers`\",\"tableChanges\":[]}\n{\"source\":{\"server\":\"inventory\"},\"position\":{\"ts_sec\":1722664430,\"file\":\"mysql-bin.000003\",\"pos\":4920,\"snapshot\":true},\"ts_ms\":1722664430341,\"databaseName\":\"inventory\",\"ddl\":\"DROP TABLE IF EXISTS `inventory`.`iceberg_namespace_properties`\",\"tableChanges\":[]}\n{\"source\":{\"server\":\"inventory\"},\"position\":{\"ts_sec\":1722664430,\"file\":\"mysql-bin.000003\",\"pos\":4920,\"snapshot\":true},\"ts_ms\":1722664430342,\"databaseName\":\"inventory\",\"ddl\":\"DROP TABLE IF EXISTS `inventory`.`iceberg_tables`\",\"tableChanges\":[]}\n{\"source\":{\"server\":\"inventory\"},\"position\":{\"ts_sec\":1722664430,\"file\":\"mysql-bin.000003\",\"pos\":4920,\"snapshot\":true},\"ts_ms\":1722664430343,\"databaseName\":\"inventory\",\"ddl\":\"DROP TABLE IF EXISTS `inventory`.`orders`\",\"tableChanges\":[]}\n{\"source\":{\"server\":\"inventory\"},\"position\":{\"ts_sec\":1722664430,\"file\":\"mysql-bin.000003\",\"pos\":4920,\"snapshot\":true},\"ts_ms\":1722664430344,\"databaseName\":\"inventory\",\"ddl\":\"DROP TABLE IF EXISTS `inventory`.`products`\",\"tableChanges\":[]}\n{\"source\":{\"server\":\"inventory\"},\"position\":{\"ts_sec\":1722664430,\"file\":\"mysql-bin.000003\",\"pos\":4920,\"snapshot\":true},\"ts_ms\":1722664430345,\"databaseName\":\"inventory\",\"ddl\":\"DROP TABLE IF EXISTS `inventory`.`products_on_hand`\",\"tableChanges\":[]}\n{\"source\":{\"server\":\"inventory\"},\"position\":{\"ts_sec\":1722664430,\"file\":\"mysql-bin.000003\",\"pos\":4920,\"snapshot\":true},\"ts_ms\":1722664430356,\"databaseName\":\"inventory\",\"ddl\":\"DROP DATABASE IF EXISTS `inventory`\",\"tableChanges\":[]}\n{\"source\":{\"server\":\"inventory\"},\"position\":{\"ts_sec\":1722664430,\"file\":\"mysql-bin.000003\",\"pos\":4920,\"snapshot\":true},\"ts_ms\":1722664430364,\"databaseName\":\"inventory\",\"ddl\":\"CREATE DATABASE `inventory` CHARSET utf8mb4 COLLATE utf8mb4_0900_ai_ci\",\"tableChanges\":[]}\n{\"source\":{\"server\":\"inventory\"},\"position\":{\"ts_sec\":1722664430,\"file\":\"mysql-bin.000003\",\"pos\":4920,\"snapshot\":true},\"ts_ms\":1722664430368,\"databaseName\":\"inventory\",\"ddl\":\"USE `inventory`\",\"tableChanges\":[]}\n{\"source\":{\"server\":\"inventory\"},\"position\":{\"ts_sec\":1722664430,\"file\":\"mysql-bin.000003\",\"pos\":4920,\"snapshot\":true},\"ts_ms\":1722664430461,\"databaseName\":\"inventory\",\"ddl\":\"CREATE TABLE `addresses` (\\n  `id` int NOT NULL AUTO_INCREMENT,\\n  `customer_id` int NOT NULL,\\n  `street` varchar(255) NOT NULL,\\n  `city` varchar(255) NOT NULL,\\n  `state` varchar(255) NOT NULL,\\n  `zip` varchar(255) NOT NULL,\\n  `type` enum('SHIPPING','BILLING','LIVING') NOT NULL,\\n  PRIMARY KEY (`id`),\\n  KEY `address_customer` (`customer_id`),\\n  CONSTRAINT `addresses_ibfk_1` FOREIGN KEY (`customer_id`) REFERENCES `customers` (`id`)\\n) ENGINE=InnoDB AUTO_INCREMENT=17 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci\",\"tableChanges\":[{\"type\":\"CREATE\",\"id\":\"\\\"inventory\\\".\\\"addresses\\\"\",\"table\":{\"defaultCharsetName\":\"utf8mb4\",\"primaryKeyColumnNames\":[\"id\"],\"columns\":[{\"name\":\"id\",\"jdbcType\":4,\"typeName\":\"INT\",\"typeExpression\":\"INT\",\"charsetName\":null,\"position\":1,\"optional\":false,\"autoIncremented\":true,\"generated\":true,\"comment\":null,\"hasDefaultValue\":false,\"enumValues\":[]},{\"name\":\"customer_id\",\"jdbcType\":4,\"typeName\":\"INT\",\"typeExpression\":\"INT\",\"charsetName\":null,\"position\":2,\"optional\":false,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":false,\"enumValues\":[]},{\"name\":\"street\",\"jdbcType\":12,\"typeName\":\"VARCHAR\",\"typeExpression\":\"VARCHAR\",\"charsetName\":\"utf8mb4\",\"length\":255,\"position\":3,\"optional\":false,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":false,\"enumValues\":[]},{\"name\":\"city\",\"jdbcType\":12,\"typeName\":\"VARCHAR\",\"typeExpression\":\"VARCHAR\",\"charsetName\":\"utf8mb4\",\"length\":255,\"position\":4,\"optional\":false,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":false,\"enumValues\":[]},{\"name\":\"state\",\"jdbcType\":12,\"typeName\":\"VARCHAR\",\"typeExpression\":\"VARCHAR\",\"charsetName\":\"utf8mb4\",\"length\":255,\"position\":5,\"optional\":false,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":false,\"enumValues\":[]},{\"name\":\"zip\",\"jdbcType\":12,\"typeName\":\"VARCHAR\",\"typeExpression\":\"VARCHAR\",\"charsetName\":\"utf8mb4\",\"length\":255,\"position\":6,\"optional\":false,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":false,\"enumValues\":[]},{\"name\":\"type\",\"jdbcType\":1,\"typeName\":\"ENUM\",\"typeExpression\":\"ENUM\",\"charsetName\":\"utf8mb4\",\"length\":1,\"position\":7,\"optional\":false,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":false,\"enumValues\":[\"'SHIPPING'\",\"'BILLING'\",\"'LIVING'\"]}],\"attributes\":[]},\"comment\":null}]}\n{\"source\":{\"server\":\"inventory\"},\"position\":{\"ts_sec\":1722664430,\"file\":\"mysql-bin.000003\",\"pos\":4920,\"snapshot\":true},\"ts_ms\":1722664430480,\"databaseName\":\"inventory\",\"ddl\":\"CREATE TABLE `customers` (\\n  `id` int NOT NULL AUTO_INCREMENT,\\n  `first_name` varchar(255) NOT NULL,\\n  `last_name` varchar(255) NOT NULL,\\n  `email` varchar(255) NOT NULL,\\n  PRIMARY KEY (`id`),\\n  UNIQUE KEY `email` (`email`)\\n) ENGINE=InnoDB AUTO_INCREMENT=1005 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci\",\"tableChanges\":[{\"type\":\"CREATE\",\"id\":\"\\\"inventory\\\".\\\"customers\\\"\",\"table\":{\"defaultCharsetName\":\"utf8mb4\",\"primaryKeyColumnNames\":[\"id\"],\"columns\":[{\"name\":\"id\",\"jdbcType\":4,\"typeName\":\"INT\",\"typeExpression\":\"INT\",\"charsetName\":null,\"position\":1,\"optional\":false,\"autoIncremented\":true,\"generated\":true,\"comment\":null,\"hasDefaultValue\":false,\"enumValues\":[]},{\"name\":\"first_name\",\"jdbcType\":12,\"typeName\":\"VARCHAR\",\"typeExpression\":\"VARCHAR\",\"charsetName\":\"utf8mb4\",\"length\":255,\"position\":2,\"optional\":false,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":false,\"enumValues\":[]},{\"name\":\"last_name\",\"jdbcType\":12,\"typeName\":\"VARCHAR\",\"typeExpression\":\"VARCHAR\",\"charsetName\":\"utf8mb4\",\"length\":255,\"position\":3,\"optional\":false,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":false,\"enumValues\":[]},{\"name\":\"email\",\"jdbcType\":12,\"typeName\":\"VARCHAR\",\"typeExpression\":\"VARCHAR\",\"charsetName\":\"utf8mb4\",\"length\":255,\"position\":4,\"optional\":false,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":false,\"enumValues\":[]}],\"attributes\":[]},\"comment\":null}]}\n{\"source\":{\"server\":\"inventory\"},\"position\":{\"ts_sec\":1722664430,\"file\":\"mysql-bin.000003\",\"pos\":4920,\"snapshot\":true},\"ts_ms\":1722664430509,\"databaseName\":\"inventory\",\"ddl\":\"CREATE TABLE `iceberg_namespace_properties` (\\n  `catalog_name` varchar(255) NOT NULL,\\n  `namespace` varchar(255) NOT NULL,\\n  `property_key` varchar(255) NOT NULL,\\n  `property_value` varchar(255) DEFAULT NULL,\\n  PRIMARY KEY (`catalog_name`,`namespace`,`property_key`)\\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci\",\"tableChanges\":[{\"type\":\"CREATE\",\"id\":\"\\\"inventory\\\".\\\"iceberg_namespace_properties\\\"\",\"table\":{\"defaultCharsetName\":\"utf8mb4\",\"primaryKeyColumnNames\":[\"catalog_name\",\"namespace\",\"property_key\"],\"columns\":[{\"name\":\"catalog_name\",\"jdbcType\":12,\"typeName\":\"VARCHAR\",\"typeExpression\":\"VARCHAR\",\"charsetName\":\"utf8mb4\",\"length\":255,\"position\":1,\"optional\":false,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":false,\"enumValues\":[]},{\"name\":\"namespace\",\"jdbcType\":12,\"typeName\":\"VARCHAR\",\"typeExpression\":\"VARCHAR\",\"charsetName\":\"utf8mb4\",\"length\":255,\"position\":2,\"optional\":false,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":false,\"enumValues\":[]},{\"name\":\"property_key\",\"jdbcType\":12,\"typeName\":\"VARCHAR\",\"typeExpression\":\"VARCHAR\",\"charsetName\":\"utf8mb4\",\"length\":255,\"position\":3,\"optional\":false,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":false,\"enumValues\":[]},{\"name\":\"property_value\",\"jdbcType\":12,\"typeName\":\"VARCHAR\",\"typeExpression\":\"VARCHAR\",\"charsetName\":\"utf8mb4\",\"length\":255,\"position\":4,\"optional\":true,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":true,\"enumValues\":[]}],\"attributes\":[]},\"comment\":null}]}\n{\"source\":{\"server\":\"inventory\"},\"position\":{\"ts_sec\":1722664430,\"file\":\"mysql-bin.000003\",\"pos\":4920,\"snapshot\":true},\"ts_ms\":1722664430518,\"databaseName\":\"inventory\",\"ddl\":\"CREATE TABLE `iceberg_tables` (\\n  `catalog_name` varchar(255) NOT NULL,\\n  `table_namespace` varchar(255) NOT NULL,\\n  `table_name` varchar(255) NOT NULL,\\n  `metadata_location` varchar(255) NOT NULL,\\n  `previous_metadata_location` varchar(255) DEFAULT NULL,\\n  PRIMARY KEY (`catalog_name`,`table_namespace`,`table_name`)\\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci\",\"tableChanges\":[{\"type\":\"CREATE\",\"id\":\"\\\"inventory\\\".\\\"iceberg_tables\\\"\",\"table\":{\"defaultCharsetName\":\"utf8mb4\",\"primaryKeyColumnNames\":[\"catalog_name\",\"table_namespace\",\"table_name\"],\"columns\":[{\"name\":\"catalog_name\",\"jdbcType\":12,\"typeName\":\"VARCHAR\",\"typeExpression\":\"VARCHAR\",\"charsetName\":\"utf8mb4\",\"length\":255,\"position\":1,\"optional\":false,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":false,\"enumValues\":[]},{\"name\":\"table_namespace\",\"jdbcType\":12,\"typeName\":\"VARCHAR\",\"typeExpression\":\"VARCHAR\",\"charsetName\":\"utf8mb4\",\"length\":255,\"position\":2,\"optional\":false,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":false,\"enumValues\":[]},{\"name\":\"table_name\",\"jdbcType\":12,\"typeName\":\"VARCHAR\",\"typeExpression\":\"VARCHAR\",\"charsetName\":\"utf8mb4\",\"length\":255,\"position\":3,\"optional\":false,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":false,\"enumValues\":[]},{\"name\":\"metadata_location\",\"jdbcType\":12,\"typeName\":\"VARCHAR\",\"typeExpression\":\"VARCHAR\",\"charsetName\":\"utf8mb4\",\"length\":255,\"position\":4,\"optional\":false,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":false,\"enumValues\":[]},{\"name\":\"previous_metadata_location\",\"jdbcType\":12,\"typeName\":\"VARCHAR\",\"typeExpression\":\"VARCHAR\",\"charsetName\":\"utf8mb4\",\"length\":255,\"position\":5,\"optional\":true,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":true,\"enumValues\":[]}],\"attributes\":[]},\"comment\":null}]}\n{\"source\":{\"server\":\"inventory\"},\"position\":{\"ts_sec\":1722664430,\"file\":\"mysql-bin.000003\",\"pos\":4920,\"snapshot\":true},\"ts_ms\":1722664430529,\"databaseName\":\"inventory\",\"ddl\":\"CREATE TABLE `orders` (\\n  `order_number` int NOT NULL AUTO_INCREMENT,\\n  `order_date` date NOT NULL,\\n  `purchaser` int NOT NULL,\\n  `quantity` int NOT NULL,\\n  `product_id` int NOT NULL,\\n  PRIMARY KEY (`order_number`),\\n  KEY `order_customer` (`purchaser`),\\n  KEY `ordered_product` (`product_id`),\\n  CONSTRAINT `orders_ibfk_1` FOREIGN KEY (`purchaser`) REFERENCES `customers` (`id`),\\n  CONSTRAINT `orders_ibfk_2` FOREIGN KEY (`product_id`) REFERENCES `products` (`id`)\\n) ENGINE=InnoDB AUTO_INCREMENT=10005 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci\",\"tableChanges\":[{\"type\":\"CREATE\",\"id\":\"\\\"inventory\\\".\\\"orders\\\"\",\"table\":{\"defaultCharsetName\":\"utf8mb4\",\"primaryKeyColumnNames\":[\"order_number\"],\"columns\":[{\"name\":\"order_number\",\"jdbcType\":4,\"typeName\":\"INT\",\"typeExpression\":\"INT\",\"charsetName\":null,\"position\":1,\"optional\":false,\"autoIncremented\":true,\"generated\":true,\"comment\":null,\"hasDefaultValue\":false,\"enumValues\":[]},{\"name\":\"order_date\",\"jdbcType\":91,\"typeName\":\"DATE\",\"typeExpression\":\"DATE\",\"charsetName\":null,\"position\":2,\"optional\":false,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":false,\"enumValues\":[]},{\"name\":\"purchaser\",\"jdbcType\":4,\"typeName\":\"INT\",\"typeExpression\":\"INT\",\"charsetName\":null,\"position\":3,\"optional\":false,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":false,\"enumValues\":[]},{\"name\":\"quantity\",\"jdbcType\":4,\"typeName\":\"INT\",\"typeExpression\":\"INT\",\"charsetName\":null,\"position\":4,\"optional\":false,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":false,\"enumValues\":[]},{\"name\":\"product_id\",\"jdbcType\":4,\"typeName\":\"INT\",\"typeExpression\":\"INT\",\"charsetName\":null,\"position\":5,\"optional\":false,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":false,\"enumValues\":[]}],\"attributes\":[]},\"comment\":null}]}\n{\"source\":{\"server\":\"inventory\"},\"position\":{\"ts_sec\":1722664430,\"file\":\"mysql-bin.000003\",\"pos\":4920,\"snapshot\":true},\"ts_ms\":1722664430544,\"databaseName\":\"inventory\",\"ddl\":\"CREATE TABLE `products` (\\n  `id` int NOT NULL AUTO_INCREMENT,\\n  `name` varchar(255) NOT NULL,\\n  `description` varchar(512) DEFAULT NULL,\\n  `weight` float DEFAULT NULL,\\n  PRIMARY KEY (`id`)\\n) ENGINE=InnoDB AUTO_INCREMENT=110 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci\",\"tableChanges\":[{\"type\":\"CREATE\",\"id\":\"\\\"inventory\\\".\\\"products\\\"\",\"table\":{\"defaultCharsetName\":\"utf8mb4\",\"primaryKeyColumnNames\":[\"id\"],\"columns\":[{\"name\":\"id\",\"jdbcType\":4,\"typeName\":\"INT\",\"typeExpression\":\"INT\",\"charsetName\":null,\"position\":1,\"optional\":false,\"autoIncremented\":true,\"generated\":true,\"comment\":null,\"hasDefaultValue\":false,\"enumValues\":[]},{\"name\":\"name\",\"jdbcType\":12,\"typeName\":\"VARCHAR\",\"typeExpression\":\"VARCHAR\",\"charsetName\":\"utf8mb4\",\"length\":255,\"position\":2,\"optional\":false,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":false,\"enumValues\":[]},{\"name\":\"description\",\"jdbcType\":12,\"typeName\":\"VARCHAR\",\"typeExpression\":\"VARCHAR\",\"charsetName\":\"utf8mb4\",\"length\":512,\"position\":3,\"optional\":true,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":true,\"enumValues\":[]},{\"name\":\"weight\",\"jdbcType\":6,\"typeName\":\"FLOAT\",\"typeExpression\":\"FLOAT\",\"charsetName\":null,\"position\":4,\"optional\":true,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":true,\"enumValues\":[]}],\"attributes\":[]},\"comment\":null}]}\n{\"source\":{\"server\":\"inventory\"},\"position\":{\"ts_sec\":1722664430,\"file\":\"mysql-bin.000003\",\"pos\":4920,\"snapshot\":true},\"ts_ms\":1722664430549,\"databaseName\":\"inventory\",\"ddl\":\"CREATE TABLE `products_on_hand` (\\n  `product_id` int NOT NULL,\\n  `quantity` int NOT NULL,\\n  PRIMARY KEY (`product_id`),\\n  CONSTRAINT `products_on_hand_ibfk_1` FOREIGN KEY (`product_id`) REFERENCES `products` (`id`)\\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci\",\"tableChanges\":[{\"type\":\"CREATE\",\"id\":\"\\\"inventory\\\".\\\"products_on_hand\\\"\",\"table\":{\"defaultCharsetName\":\"utf8mb4\",\"primaryKeyColumnNames\":[\"product_id\"],\"columns\":[{\"name\":\"product_id\",\"jdbcType\":4,\"typeName\":\"INT\",\"typeExpression\":\"INT\",\"charsetName\":null,\"position\":1,\"optional\":false,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":false,\"enumValues\":[]},{\"name\":\"quantity\",\"jdbcType\":4,\"typeName\":\"INT\",\"typeExpression\":\"INT\",\"charsetName\":null,\"position\":2,\"optional\":false,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":false,\"enumValues\":[]}],\"attributes\":[]},\"comment\":null}]}\n"}}"#
        );

        let input = File::open("../testdata/mysql/input2.txt")?;

        ingest(&mut BufReader::new(input), plugin.clone(), &airbyte_catalog).await?;

        let orders_table = if let Tabular::Table(table) = catalog
            .clone()
            .load_tabular(&Identifier::parse("inventory.orders", None)?)
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

    #[tokio::test]
    async fn test_s3() -> Result<(), Error> {
        let tempdir = tempdir()?;

        let config_path = tempdir.path().join("config.json");

        let mut config_file = File::create(config_path.clone())?;

        config_file.write_all(
            (r#"
            {
            "catalogUrl": ""#
                .to_string()
                + tempdir.path().to_str().unwrap()
                + r#""
            }
        "#)
            .as_bytes(),
        )?;

        let plugin =
            Arc::new(FileDestinationPlugin::new(config_path.as_path().to_str().unwrap()).await?);

        let airbyte_catalog =
            configure_catalog("../testdata/s3/discover.json", plugin.clone()).await?;

        let input = File::open("../testdata/s3/input1.txt")?;

        ingest(&mut BufReader::new(input), plugin.clone(), &airbyte_catalog).await?;

        let catalog = plugin.catalog().await?;

        let orders_table = if let Tabular::Table(table) = catalog
            .clone()
            .load_tabular(&Identifier::parse("default.test", None)?)
            .await?
        {
            Ok(table)
        } else {
            Err(anyhow!("Not a table"))
        }?;

        let manifests = orders_table.manifests(None, None).await?;

        assert_eq!(manifests[0].added_rows_count.unwrap(), 8);

        let stream_state = orders_table
            .metadata()
            .properties
            .get("airbyte.stream_state")
            .expect("Failed to get bookmark");

        assert_eq!(
            stream_state,
            r#"{"_ab_source_file_last_modified":"2024-10-09T11:17:16Z","history":{"2024-10-09":["test.csv"]}}"#
        );

        Ok(())
    }
}
