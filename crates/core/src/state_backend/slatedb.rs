use denormalized_common::DenormalizedError;
use object_store::{local::LocalFileSystem, path::Path};
use slatedb::error::SlateDBError;
use slatedb::{config::DbOptions, db::Db};
use std::sync::{Arc, OnceLock};

static GLOBAL_SLATEDB: OnceLock<Arc<SlateDBWrapper>> = OnceLock::new();

pub async fn initialize_global_slatedb(path: &str) -> Result<(), DenormalizedError> {
    let backend = SlateDBWrapper::initialize(path).await.unwrap();
    GLOBAL_SLATEDB
        .set(Arc::new(backend))
        .map_err(|_| DenormalizedError::RocksDB("Global SlateDB already initialized".to_string()))
}

pub fn get_global_slatedb() -> Result<Arc<SlateDBWrapper>, DenormalizedError> {
    GLOBAL_SLATEDB
        .get()
        .cloned()
        .ok_or_else(|| DenormalizedError::RocksDB("Global SlateDB not initialized".to_string()))
}

pub struct SlateDBWrapper {
    db: Arc<Db>,
}

//TODO: This is a WIP. the comments will be cleaned up 10/19/2024
impl SlateDBWrapper {
    // Function to initialize the SlateDB instance
    pub async fn initialize(path_str: &str) -> Result<Self, SlateDBError> {
        // let os: Arc<dyn ObjectStore> = Arc::new(
        //     AmazonS3Builder::new()
        //         .with_allow_http(true)
        //         .with_endpoint("http://localhost:4566")
        //         .with_access_key_id("test")
        //         .with_secret_access_key("test")
        //         .with_bucket_name("slatedb")
        //         .with_region("us-east-1")
        //         .with_conditional_put(S3ConditionalPut::Dynamo(DynamoCommit::new(
        //             "slatedb".to_string(),
        //         )))
        //         .build()
        //         .expect("failed to create object store"),
        // );

        let os = Arc::new(LocalFileSystem::new());
        let db_options = DbOptions::default();
        // let compactor_opts = db_options
        //     .clone()
        //     .compactor_options
        //     .map(|co| co.with_compactor_handle(runtime.handle().clone()))
        //     .unwrap();

        let path = Path::from(path_str);

        //let db_options_with_new_compactor = db_options.with_compactor_options(compactor_opts);
        // Capture the runtime handle and block on database initialization

        let db = Db::open_with_opts(path, db_options, os).await?;

        Ok(SlateDBWrapper { db: Arc::new(db) })
    }

    // Non-async method to perform a put operation
    pub fn put(&self, key: Vec<u8>, value: Vec<u8>) {
        let db_clone = self.db.clone();
        //let handle = self.runtime_handle.clone();

        // Use the runtime handle to spawn a blocking task for the put operation
        // self.runtime.spawn_blocking(move || {
        let rt = tokio::runtime::Handle::current();
        rt.spawn(async move {
            db_clone.put(&key, &value).await;
        });
        //}); //.expect("Failed to spawn blocking task");
    }

    // Non-async method to perform a get operation
    pub async fn get(&self, key: Vec<u8>) -> Option<Vec<u8>> {
        let db_clone = self.db.clone();

        // Block on the get operation using the runtime handle
        let result = db_clone.get(&key).await.unwrap_or(None);

        result.map(|bytes| bytes.to_vec())
    }

    pub async fn close(&self) -> Result<(), SlateDBError> {
        let db_clone = self.db.clone();
        db_clone.close().await
    }
}
