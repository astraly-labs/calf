use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

#[derive(Debug)]
pub struct Db {
    db: Arc<Mutex<HashMap<String, HashMap<String, Vec<u8>>>>>, // Thread-safe storage
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Serialization error: {0}")]
    Bincode(#[from] bincode::Error),
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
    const COUNT: usize = 3;

    pub const ALL: &'static [Self] = {
        use Column::*;
        &[Batches, Headers, Digests]
    };

    fn iter() -> impl Iterator<Item = Self> {
        Self::ALL.iter().copied()
    }

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
    pub fn new(_: PathBuf) -> Result<Self, anyhow::Error> {
        let mut db = HashMap::new();
        for column in Column::iter() {
            db.insert(column.as_str().to_string(), HashMap::new());
        }
        Ok(Self {
            db: Arc::new(Mutex::new(db)),
        })
    }

    pub fn insert<T>(&self, column: Column, key: &str, value: T) -> Result<(), Error>
    where
        T: serde::Serialize,
    {
        let value = bincode::serialize(&value)?;
        let mut db_lock = self.db.lock().unwrap();
        let column_map = db_lock.get_mut(column.as_str()).unwrap();
        column_map.insert(key.to_string(), value);
        Ok(())
    }

    pub fn get<T>(&self, column: Column, key: &str) -> Result<Option<T>, Error>
    where
        T: serde::de::DeserializeOwned,
    {
        let db_lock = self.db.lock().unwrap();
        if let Some(column_map) = db_lock.get(column.as_str()) {
            if let Some(value) = column_map.get(key) {
                let deserialized: T = bincode::deserialize(value)?;
                return Ok(Some(deserialized));
            }
        }
        Ok(None)
    }

    pub fn remove<T>(&self, column: Column, key: &str) -> Result<Option<T>, Error>
    where
        T: serde::de::DeserializeOwned,
    {
        let mut db_lock = self.db.lock().unwrap();
        if let Some(column_map) = db_lock.get_mut(column.as_str()) {
            if let Some(value) = column_map.remove(key) {
                let deserialized: T = bincode::deserialize(&value)?;
                return Ok(Some(deserialized));
            }
        }
        Ok(None)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_db() {
        let db = Db::new("/tmp/test_db_3".into()).unwrap();
        db.insert(Column::Batches, "key", 42).unwrap();
        assert_eq!(db.get::<i32>(Column::Batches, "key").unwrap(), Some(42));
        assert_eq!(db.remove::<i32>(Column::Batches, "key").unwrap(), Some(42));
        assert_eq!(db.get::<i32>(Column::Batches, "key").unwrap(), None);
    }
}
