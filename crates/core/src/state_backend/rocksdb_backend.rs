use std::{
    env,
    sync::{Arc, OnceLock},
};

use denormalized_common::error::{DenormalizedError, Result};
use log::debug;
use rocksdb::{
    BoundColumnFamily, ColumnFamilyDescriptor, DBWithThreadMode, MultiThreaded, Options, DB,
};

pub struct RocksDBBackend {
    db: DBWithThreadMode<MultiThreaded>,
}

impl RocksDBBackend {
    pub fn new(path: &str) -> Result<Self, DenormalizedError> {
        let dir = env::temp_dir();
        let db_path = format!("{}{}", dir.display(), path);
        debug!("Opening rocksdb at {}", db_path);

        let mut db_opts = Options::default();
        db_opts.create_if_missing(true);

        // List all column families in the existing database
        let cf_names = DB::list_cf(&db_opts, &db_path).map_err(|e| {
            DenormalizedError::RocksDB(format!("Failed to list column families: {}", e))
        })?;

        if cf_names.is_empty() {
            // If no column families, open the DB normally
            let db = DBWithThreadMode::<MultiThreaded>::open(&db_opts, &db_path).map_err(|e| {
                DenormalizedError::RocksDB(format!("Failed to open RocksDB: {}", e))
            })?;
            Ok(RocksDBBackend { db })
        } else {
            // If column families exist, open the DB with all existing column families
            let cf_descriptors: Vec<ColumnFamilyDescriptor> = cf_names
                .iter()
                .map(|name| ColumnFamilyDescriptor::new(name, Options::default()))
                .collect();

            let db = DBWithThreadMode::<MultiThreaded>::open_cf_descriptors(
                &db_opts,
                &db_path,
                cf_descriptors,
            )
            .map_err(|e| {
                DenormalizedError::RocksDB(format!(
                    "Failed to open RocksDB with column families: {}",
                    e
                ))
            })?;
            Ok(RocksDBBackend { db })
        }
    }

    pub fn create_cf(&self, namespace: &str) -> Result<(), DenormalizedError> {
        let cf_opts: Options = Options::default();
        DBWithThreadMode::<MultiThreaded>::create_cf(&self.db, namespace, &cf_opts)
            .map_err(|e| DenormalizedError::RocksDB(e.into_string()))?;
        //self.namespaces.insert(namespace.to_string());
        Ok(())
    }

    pub fn get_cf(&self, namespace: &str) -> Result<Arc<BoundColumnFamily>, DenormalizedError> {
        self.db
            .cf_handle(namespace)
            .ok_or_else(|| DenormalizedError::RocksDB("namespace does not exist.".to_string()))
    }

    fn namespaced_key(&self, namespace: &str, key: &[u8]) -> Vec<u8> {
        let mut nk: Vec<u8> = namespace.as_bytes().to_vec();
        nk.push(b':');
        nk.extend_from_slice(key);
        nk
    }

    pub fn destroy(&self) -> Result<(), DenormalizedError> {
        let _ret = DB::destroy(&Options::default(), self.db.path());
        Ok(())
    }

    pub fn put_state(
        &self,
        namespace: &str,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<(), DenormalizedError> {
        // if !self.namespaces.contains(namespace) {
        //     return Err(StateBackendError {
        //         message: "Namespace does not exist.".into(),
        //     });
        // }
        let cf: Arc<BoundColumnFamily> = self.get_cf(namespace)?;
        let namespaced_key: Vec<u8> = self.namespaced_key(namespace, &key);
        self.db
            .put_cf(&cf, namespaced_key, value)
            .map_err(|e| DenormalizedError::RocksDB(e.to_string()))
    }

    pub fn get_state(
        &self,
        namespace: &str,
        key: Vec<u8>,
    ) -> Result<Option<Vec<u8>>, DenormalizedError> {
        let cf: Arc<BoundColumnFamily> = self.get_cf(namespace)?;
        let namespaced_key: Vec<u8> = self.namespaced_key(namespace, &key);

        match self
            .db
            .get_cf(&cf, namespaced_key)
            .map_err(|e| DenormalizedError::RocksDB(e.to_string()))?
        {
            Some(serialized_value) => Ok(Some(serialized_value)),
            None => Ok(None),
        }
    }

    pub async fn delete_state(
        &self,
        namespace: &str,
        key: Vec<u8>,
    ) -> Result<(), DenormalizedError> {
        let cf: Arc<BoundColumnFamily> = self.get_cf(namespace)?;
        let namespaced_key: Vec<u8> = self.namespaced_key(namespace, &key);

        self.db
            .delete_cf(&cf, namespaced_key)
            .map_err(|e| DenormalizedError::RocksDB(e.to_string()))?;
        Ok(())
    }
}

static GLOBAL_ROCKSDB: OnceLock<Arc<RocksDBBackend>> = OnceLock::new();

pub fn initialize_global_rocksdb(path: &str) -> Result<(), DenormalizedError> {
    let backend = RocksDBBackend::new(path)?;
    GLOBAL_ROCKSDB.set(Arc::new(backend)).map_err(|_| {
        DenormalizedError::RocksDB("Global RocksDBBackend already initialized".to_string())
    })
}

pub fn get_global_rocksdb() -> Result<Arc<RocksDBBackend>, DenormalizedError> {
    GLOBAL_ROCKSDB.get().cloned().ok_or_else(|| {
        DenormalizedError::RocksDB("Global RocksDBBackend not initialized".to_string())
    })
}
