use super::traits::{AsBytes, Hash, Random};
use derive_more::derive::Constructor;
use serde::{Deserialize, Serialize};

const RANDOM_ITEM_SIZE: usize = 32;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Constructor, Default, Hash)]
pub struct Batch<T>(pub Vec<T>)
where
    T: AsBytes + Clone + Hash;

impl<T> Batch<T>
where
    T: AsBytes + Hash + Clone,
{
    pub fn data(&self) -> Vec<u8> {
        self.0.iter().flat_map(|t| t.bytes()).collect()
    }
    pub fn len(&self) -> usize {
        self.0.len()
    }
}

impl<T> AsBytes for Batch<T>
where
    T: AsBytes + Hash + Clone,
{
    fn bytes(&self) -> Vec<u8> {
        self.0.iter().flat_map(|t| t.bytes()).collect()
    }
}

impl<T> Random for Batch<T>
where
    T: AsBytes + Hash + Clone + Random,
{
    fn random(size: usize) -> Self {
        let data = (0..size)
            .into_iter()
            .map(|_| T::random(RANDOM_ITEM_SIZE))
            .collect();
        Self(data)
    }
}
