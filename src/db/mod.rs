use std::path::PathBuf;

use sled::{transaction, Db as SledDb};

#[derive(Debug)]
pub struct Db {
    #[allow(dead_code)]
    db: SledDb,
    columns: [sled::Tree; Column::COUNT],
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Sled error: {0}")]
    Sled(#[from] sled::Error),
    #[error("Transaction error: {0}")]
    Transaction(#[from] transaction::TransactionError),
    #[error("Bincode error: {0}")]
    Bincode(#[from] bincode::Error),
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum Column {
    Batches,
    Headers,
}

#[derive(Debug, thiserror::Error)]
pub enum DbError {
    #[error("Invalid columns number")]
    InvalidColumnsNumber,
}

impl Column {
    const COUNT: usize = 1;

    pub const ALL: &'static [Self] = {
        use Column::*;
        &[Batches]
    };

    fn iter() -> impl Iterator<Item = Self> {
        Self::ALL.iter().copied()
    }

    fn as_str(&self) -> &'static str {
        match self {
            Column::Batches => "batches",
            Column::Headers => "headers",
        }
    }

    fn as_usize(&self) -> usize {
        *self as usize
    }
}

#[allow(dead_code)]
impl Db {
    pub fn new(path: PathBuf) -> Result<Self, anyhow::Error> {
        let db = sled::open(path)?;
        let columns: [sled::Tree; Column::COUNT] = match Column::iter()
            .map(|column| db.open_tree(column.as_str()))
            .collect::<Result<Vec<_>, _>>()?
            .try_into()
        {
            Ok(columns) => columns,
            Err(_) => return Err(DbError::InvalidColumnsNumber.into()),
        };
        Ok(Self { db, columns })
    }

    pub fn insert<T>(&self, column: Column, key: &str, value: T) -> Result<(), Error>
    where
        T: serde::Serialize,
    {
        let value = bincode::serialize(&value)?;
        self.columns[column.as_usize()].insert(key, value)?;
        Ok(())
    }

    pub fn get<T>(&self, column: Column, key: &str) -> Result<Option<T>, Error>
    where
        T: serde::de::DeserializeOwned,
    {
        let value = self.columns[column.as_usize()].get(key)?;
        match value {
            Some(value) => Ok(Some(bincode::deserialize(&value)?)),
            None => Ok(None),
        }
    }

    pub fn remove<T>(&self, column: Column, key: &str) -> Result<Option<T>, Error>
    where
        T: serde::de::DeserializeOwned,
    {
        let value = self.columns[column.as_usize()].remove(key)?;
        match value {
            Some(value) => Ok(Some(bincode::deserialize(&value)?)),
            None => Ok(None),
        }
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
