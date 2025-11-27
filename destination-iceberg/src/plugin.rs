use std::{collections::HashMap, sync::Arc};

use airbyte_protocol::message::SyncMode;
use async_trait::async_trait;
use iceberg_rust::catalog::Catalog;
use serde::{Deserialize, Serialize};

use crate::error::Error;

#[async_trait]
pub trait DestinationPlugin: Send + Sync {
    async fn catalog(&self) -> Result<Arc<dyn Catalog>, Error>;
    fn namespace(&self) -> Option<&str>;
    fn streams(&self) -> &HashMap<String, StreamConfig>;
    fn bucket(&self) -> Option<&str>;
    fn branch(&self) -> Option<&str>;
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BaseConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub streams: HashMap<String, StreamConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub branch: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct StreamConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sync_mode: Option<SyncMode>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cursor_field: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition_by: Option<HashMap<String, String>>,
}

#[cfg(test)]
mod tests {

    use dashtool_common::ObjectStoreConfig;
    use serde::{Deserialize, Serialize};

    use super::BaseConfig;

    #[derive(Debug, Serialize, Deserialize)]
    pub struct Config {
        #[serde(flatten)]
        pub base: BaseConfig,
        #[serde(flatten)]
        pub object_store: ObjectStoreConfig,
    }

    #[test]
    fn test_config() {
        let config: Config = serde_json::from_str(
            r#"
            {
                "streams": {"hello": { "identifier": "world", "syncMode": "incremental" }}
            }
            "#,
        )
        .expect("Failed to parse config");

        let ObjectStoreConfig::Memory = config.object_store else {
            panic!("Wrong object_store type")
        };
    }
}
