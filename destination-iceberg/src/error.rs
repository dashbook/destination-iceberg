use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("{0} could not ve found.")]
    NotFound(String),
    #[error("{0} is invalid.")]
    Invalid(String),
    #[error(transparent)]
    Serde(#[from] serde_json::Error),
    #[error(transparent)]
    Chrono(#[from] chrono::ParseError),
    #[error(transparent)]
    IO(#[from] std::io::Error),
    #[error(transparent)]
    Arrow(#[from] arrow::error::ArrowError),
    #[error(transparent)]
    Iceberg(#[from] iceberg_rust::error::Error),
    #[error(transparent)]
    IcebergSpec(#[from] iceberg_rust::spec::error::Error),
    #[error(transparent)]
    FuturesChannel(#[from] futures::channel::mpsc::SendError),
    #[error(transparent)]
    Join(#[from] tokio::task::JoinError),
    #[error(transparent)]
    ObjectStore(#[from] object_store::Error),
    #[error(transparent)]
    Anyhow(#[from] anyhow::Error),
    #[error("The message value doesn't conform to the provided schema.")]
    SchemaValidation,
    #[error("The stream has to start with a schema message.")]
    NoSchema,
    #[error("unknown data store error")]
    Unknown,
}
