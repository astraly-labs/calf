use anyhow::Context;
use rocksdb::{Env, Options, DB};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::{Arc, Mutex};

#[derive(Debug)]
pub struct Db {
    db: Arc<Mutex<DB>>,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Serialization error: {0}")]
    Bincode(#[from] bincode::Error),
    #[error("RocksDB error: {0}")]
    RocksDB(#[from] rocksdb::Error),
    #[error("Key not found")]
    KeyNotFound,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum Column {
    Batches,
    Headers,
    Digests,
}

impl Column {
    pub const ALL: &'static [Self] = {
        use Column::*;
        &[Batches, Headers, Digests]
    };

    fn as_str(&self) -> &'static str {
        match self {
            Column::Batches => "batches",
            Column::Headers => "headers",
            Column::Digests => "digests",
        }
    }
}

#[allow(dead_code)]
impl Db {
    pub fn new(path: PathBuf) -> anyhow::Result<Self> {
        let mut options = Self::rocksdb_global_options()?;
        options.create_if_missing(true);
        options.create_missing_column_families(true);

        let db = DB::open_cf(&options, path, Column::ALL.iter().map(Column::as_str))?;
        Ok(Self {
            db: Arc::new(Mutex::new(db)),
        })
    }
    pub fn rocksdb_global_options() -> anyhow::Result<Options> {
        let mut options = Options::default();
        options.create_if_missing(true);
        options.create_missing_column_families(true);
        let cores = std::thread::available_parallelism()
            .map(|e| e.get() as i32)
            .unwrap_or(1);
        options.increase_parallelism(cores);
        options.set_max_background_jobs(cores);

        options.set_atomic_flush(true);
        options.set_max_subcompactions(cores as _);

        // Safe unwrap because we know the string is valid
        options
            .set_max_log_file_size(byte_unit::Byte::from_str("10 MiB").unwrap().as_u64() as usize);
        options.set_max_open_files(2048);
        options.set_keep_log_file_num(3);
        options.set_log_level(rocksdb::LogLevel::Warn);

        let mut env = Env::new().context("Creating rocksdb env")?;
        // env.set_high_priority_background_threads(cores); // flushes
        env.set_low_priority_background_threads(cores); // compaction

        options.set_env(&env);

        Ok(options)
    }
    pub fn insert<T>(&self, column: Column, key: &str, value: T) -> Result<(), Error>
    where
        T: Serialize,
    {
        let value = bincode::serialize(&value)?;
        let db = self.db.lock().unwrap();
        let cf = db.cf_handle(column.as_str()).ok_or(Error::KeyNotFound)?;
        db.put_cf(cf, key.as_bytes(), value)?;
        Ok(())
    }

    pub fn get<T>(&self, column: Column, key: &str) -> Result<Option<T>, Error>
    where
        T: for<'de> Deserialize<'de>,
    {
        let db = self.db.lock().unwrap();
        let cf = db.cf_handle(column.as_str()).ok_or(Error::KeyNotFound)?;
        if let Some(value) = db.get_cf(cf, key.as_bytes())? {
            let deserialized: T = bincode::deserialize(&value)?;
            return Ok(Some(deserialized));
        }
        Ok(None)
    }

    pub fn remove<T>(&self, column: Column, key: &str) -> Result<Option<T>, Error>
    where
        T: for<'de> Deserialize<'de>,
    {
        let db = self.db.lock().unwrap();
        let cf = db.cf_handle(column.as_str()).ok_or(Error::KeyNotFound)?;
        if let Some(value) = db.get_cf(cf, key.as_bytes())? {
            let deserialized: T = bincode::deserialize(&value)?;
            db.delete_cf(cf, key.as_bytes())?;
            return Ok(Some(deserialized));
        }
        Ok(None)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_db() {
        let db = Db::new("/tmp/test_db_rocksdb".into()).unwrap();
        db.insert(Column::Batches, "key", 42).unwrap();
        assert_eq!(db.get::<i32>(Column::Batches, "key").unwrap(), Some(42));
        assert_eq!(db.remove::<i32>(Column::Batches, "key").unwrap(), Some(42));
        assert_eq!(db.get::<i32>(Column::Batches, "key").unwrap(), None);
    }
}
