use deltalake::operations::create::CreateBuilder;
use deltalake::{protocol::SaveMode, SchemaDataType, SchemaField, DeltaOps};
use deltalake::{open_table_with_storage_options};
use std::collections::HashMap;
use std::env;
use std::fmt::Debug;
use datafusion::prelude::{col, CsvReadOptions, SessionContext};
use uuid::Uuid;


#[derive(Default, Debug)]
pub struct StorageOptions {
    pub allow_http: String,
    pub endpoint_url: String,
    pub region: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub bucket_name: String,
}

impl StorageOptions {
    fn new() -> Self {
        Self {
            allow_http: env::var("ALLOW_HTTP").expect("Set ALLOW_HTTP env variable"),
            endpoint_url: env::var("MINIO_URL").expect("Set MINIO_URL env variable"),
            region: env::var("MINIO_STORAGE_REGION").expect("Set MINIO_STORAGE_REGION env variable"),
            access_key_id: env::var("MINIO_LOGIN").expect("Set MINIO_LOGIN env variable"),
            secret_access_key: env::var("MINIO_PASSWORD").expect("Set MINIO_PASSWORD env variable"),
            bucket_name: env::var("MINIO_SOURCE_BUCKET").expect("Set MINIO_SOURCE_BUCKET env variable"),
        }
    }

    fn to_hashmap(&self) -> HashMap<String, String> {
        let mut map = HashMap::new();
        map.insert("allow_http".to_string(), self.allow_http.clone());
        map.insert("endpoint_url".to_string(), self.endpoint_url.clone());
        map.insert("region".to_string(), self.region.clone());
        map.insert("AWS_REGION".to_string(), self.region.clone());
        map.insert("access_key_id".to_string(), self.access_key_id.clone());
        map.insert("secret_access_key".to_string(), self.secret_access_key.clone());
        map.insert("bucket_name".to_string(), self.bucket_name.clone());
        map
    }
}

#[tokio::main]
async fn main() {
    // table uri
    let id = Uuid::new_v4();
    let table_uri = format!("s3://sequencedatain/minimal_example/{id}");

    // create a DeltaTable
    let mut schema: Vec<SchemaField> = Vec::new();
    schema.push(
        SchemaField::new(
            "__id".to_string(),
            SchemaDataType::primitive("string".to_string()),
            false,
            HashMap::new(),
        )
    );
    schema.push(
        SchemaField::new(
            "__createdat".to_string(),
            SchemaDataType::primitive("timestamp".to_string()),
            false,
            HashMap::new(),
        )
    );
    schema.push(
        SchemaField::new(
            "__updatedat".to_string(),
            SchemaDataType::primitive("timestamp".to_string()),
            false,
            HashMap::new(),
        )
    );
    let _table = CreateBuilder::new()
        .with_location(&table_uri)
        .with_storage_options(StorageOptions::new().to_hashmap())
        .with_columns(schema)
        .with_save_mode(SaveMode::ErrorIfExists)
        .await.unwrap();

    // open the just-created DeltaTable
    let target_table = open_table_with_storage_options(&table_uri, StorageOptions::new().to_hashmap()).await.unwrap();

    // open the file with data to merge as datafusion DataFrame
    let ctx = SessionContext::new();
    let source_df = ctx.read_csv("minimal.csv", CsvReadOptions::new().delimiter(b';').has_header(true)).await.unwrap();
    source_df.clone()
        .limit(0, Some(50))
        .expect("Limit failed")
        .show()
        .await
        .expect("showing dataframe should succeed");

    // FAILING HERE
    let (_table, _metrics) = DeltaOps(target_table)
        .merge(source_df, col("__id").eq(col("source.__id")))
        .with_source_alias("source")
        .await.unwrap();
}
