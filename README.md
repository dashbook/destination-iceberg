
# Airbyte destination for Iceberg Tables

This is a Airbyte destination that loads data from Airbyte sources into [Iceberg tables](https://iceberg.apache.org/). This reposotory provides mulitple Airbyte destinations, one for each iceberg catalog type (REST, SQL, ...).
These are:

- [SQL catalog destination](/destination-iceberg-sql/README.md)

## Features

- Creates Iceberg tables automatically if they don't exist
- Incrementally loads versions into Iceberg tables
- Converts Airbyte source schemas into Iceberg table schemas
- Validates records against the Airbyte source schema
- Generates metadata about syncs for data governance 

## Usage

The target ingests Airbyte messages into the Iceberg tables and stores the state in the `airbyte.shared_state` and `airbyte.stream_state` property.

### Sync mode

To run:

```bash
destination-iceberg-sql --write --config config.json --catalog catalog.json
```

## Configuration

Example:

```json
{
    "catalogName": "bronze",
    "catalogUrl": "postgres://postgres:postgres@postgres:5432",
    "bucket": "s3://example-postgres",
    "awsRegion": "us-east-1",
    "awsAccessKeyId": "AKIAIOSFODNN7EXAMPLE",
    "awsSecretAccessKey": "$AWS_SECRET_ACCESS_KEY",
    "awsEndpoint": "http://localstack:4566",
    "awsAllowHttp": "true"
}
```

The configuration consists of 3 parts:
- General parameters
- Catalog parameters
- Object store parameters

### General parameters

The general parameters apply to each catalog and object store.

| Parameter | Description | 
|-|-|  
| `streams` | A map of streams to replicate. Each stream is a map with the fields: `identifier`, `replicationMethod`(optional) |
| `bucket` (optional) | Object store bucket where the iceberg tables should be stored (optional) |



### Catalog parameters

Only one set of catalog parameters should be used in the configuration. Choose which catalog you want to use.

#### SQL catalog

| Parameter | Description |
|-|-|  
| `catalogName` | The name of the catalog |
| `catalogUrl` | The connection url of the catalog |


### Object store parameters

Only one set of object store parameters should be used in the configuration. Choose which object store you want to use.

#### AWS S3

| Parameter | Description |
|-|-|  
| `awsRegion` | The region of the bucket |
| `awsAccessKeyId` | The access key id |
| `awsSecretAccessKey` | The secret access key |
| `awsEndpoint` (optional) | The endpoint of the object store |
| `awsAllowHttp` (optional) | Allow http connections to the object store |



## Docker containers

- [dashbook/destination-iceberg:sql](https://hub.docker.com/r/dashbook/destination-iceberg)

## Contributing

Feel free to open issues for any feedback or ideas! PRs are welcome.

