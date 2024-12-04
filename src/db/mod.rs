use rocksdb::{Options, DB};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
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
    //const COUNT: usize = 3;

    pub const ALL: &'static [Self] = {
        use Column::*;
        &[Batches, Headers, Digests]
    };

    // fn iter() -> impl Iterator<Item = Self> {
    //     Self::ALL.iter().copied()
    // }

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
    pub fn new(path: PathBuf) -> Result<Self, anyhow::Error> {
        let mut options = Options::default();
        options.create_if_missing(true);
        options.create_missing_column_families(true);

        let db = DB::open_cf(&options, path, Column::ALL.iter().map(Column::as_str))?;
        Ok(Self {
            db: Arc::new(Mutex::new(db)),
        })
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
