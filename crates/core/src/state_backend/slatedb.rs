use denormalized_common::DenormalizedError;
use log::debug;
use object_store::{
    aws::{AmazonS3Builder, DynamoCommit, S3ConditionalPut},
    local::LocalFileSystem,
    memory::InMemory,
    path::Path,
    ObjectStore,
};
use slatedb::{config::DbOptions, db::Db};
use std::{
    sync::{Arc, Mutex, OnceLock},
    thread,
};
use tokio::{runtime::Runtime, task};

async fn get_slate_db_backend(path_str: &str) -> Db {
    let os: Arc<dyn ObjectStore> = Arc::new(
        AmazonS3Builder::new()
            // These will be different if you are using real AWS
            .with_allow_http(true)
            .with_endpoint("http://localhost:4566")
            .with_access_key_id("test")
            .with_secret_access_key("test")
            .with_bucket_name("slatedb")
            .with_region("us-east-1")
            .with_conditional_put(S3ConditionalPut::Dynamo(DynamoCommit::new(
                "slatedb".to_string(),
            )))
            .build()
            .expect("failed to create object store"),
    );
    let db_options = DbOptions::default();
    //debug!("DBOptions {}", db_options);

    let path = Path::from(path_str);

    let os = InMemory::new();
    // Spawn a new thread
    let db = Db::open_with_opts(path, db_options, Arc::new(os))
        .await
        .expect("failed to open db");

    db
}

static GLOBAL_SLATEDB: OnceLock<Arc<Db>> = OnceLock::new();

pub async fn initialize_global_slatedb(path: &str) -> Result<(), DenormalizedError> {
    let backend = get_slate_db_backend(path).await;
    GLOBAL_SLATEDB
        .set(Arc::new(backend))
        .map_err(|_| DenormalizedError::RocksDB("Global SlateDB already initialized".to_string()))
}

pub fn get_global_slatedb() -> Result<Arc<Db>, DenormalizedError> {
    GLOBAL_SLATEDB
        .get()
        .cloned()
        .ok_or_else(|| DenormalizedError::RocksDB("Global SlateDB not initialized".to_string()))
}
