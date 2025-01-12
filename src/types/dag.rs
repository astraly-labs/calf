use derive_more::derive::Constructor;
use getset::{CopyGetters, Getters};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

use super::certificate::Certificate;
use super::traits::{AsBytes, AsHex, Hash};

#[derive(Serialize, Deserialize, Debug, Clone, CopyGetters)]
/// A particular type of DAG where vertices are ordonned by layers. vertices from each layer can only have direct parents from previous layers and the data of each vertex is unique.
pub struct Dag<T>
where
    T: Hash + AsBytes + Clone,
{
    vertices: HashMap<String, Vertex<T>>,
    vertices_by_layers: HashMap<u64, HashSet<String>>,
    #[getset(get_copy = "pub")]
    height: u64,
    #[getset(get_copy = "pub")]
    base_layer: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone, Constructor, Getters)]
/// A particular type of vertex for a particular type of DAG.
pub struct Vertex<T>
where
    T: Hash + AsBytes + Clone,
{
    #[getset(get = "pub")]
    data: T,
    #[getset(get = "pub")]
    layer: u64,
    #[getset(get = "pub")]
    parents: HashSet<String>,
    #[getset(get = "pub")]
    id: String,
}

impl<T> Dag<T>
where
    T: Hash + AsBytes + Clone,
{
    /// Create a new DAG with a base layer value (can be anything).
    pub fn new(base_layer: u64) -> Self {
        Self {
            vertices: HashMap::new(),
            vertices_by_layers: HashMap::new(),
            height: base_layer,
            base_layer,
        }
    }
    pub fn new_with_root(base_layer: u64, root: T) -> Self {
        let vertex = Vertex::from_data(root, base_layer, HashSet::new());
        let mut dag = Self::new(base_layer);
        dag.insert(vertex).unwrap();
        dag
    }
    /// Check if a vertex has all its parents in the DAG, returning an error containing the missing parents if not.
    pub fn check_parents(&self, vertex: &Vertex<T>) -> Result<(), DagError> {
        // Base layer (0) and layer 1 vertices don't require parents
        if vertex.layer <= self.base_layer || vertex.layer == 1 {
            if vertex.parents.is_empty() {
                return Ok(());
            }
        }

        // For other layers, check if all parents exist in any previous layer
        let found_parents: HashSet<_> = (self.base_layer..vertex.layer)
            .rev()
            .flat_map(|layer| self.vertices_by_layers.get(&layer).into_iter().flatten())
            .collect();

        if vertex.parents.iter().all(|p| found_parents.contains(p)) {
            Ok(())
        } else {
            let missing: HashSet<_> = vertex
                .parents
                .iter()
                .filter(|p| !found_parents.contains(*p))
                .cloned()
                .collect();
            Err(DagError::MissingParents(missing))
        }
    }
    /// Insert a vertex in the DAG, returning an error if its parents are missing but inserting it anyway.
    pub fn insert(&mut self, vertex: Vertex<T>) -> Result<(), DagError> {
        let res = self.check_parents(&vertex);
        let id = vertex.id.clone();
        let layer = vertex.layer;
        self.vertices.insert(id.clone(), vertex);
        self.vertices_by_layers.entry(layer).or_default().insert(id);
        if layer > self.height {
            self.height = layer;
        }
        res
    }
    /// Insert a vertiex in the DAG only if its parents are already in the DAG, else return an error containing the missing parents.
    pub fn insert_checked(&mut self, vertex: Vertex<T>) -> Result<(), DagError> {
        self.check_parents(&vertex)?;
        self.insert(vertex)
    }
    /// Get all the vertices for a given layer: if the layer is not yet existing, return an empty vector.
    pub fn layer_vertices(&self, layer: u64) -> Vec<&Vertex<T>> {
        self.vertices_by_layers
            .get(&layer)
            .map(|keys| keys.iter().flat_map(|key| self.vertices.get(key)).collect())
            .unwrap_or_default()
    }
    /// Get the number of vertices belonging to a given layer.
    pub fn layer_size(&self, layer: u64) -> usize {
        self.vertices_by_layers
            .get(&layer)
            .map(|keys| keys.len())
            .unwrap_or(0)
    }
    /// Get the cloned data of all vertices for a given layer.
    pub fn layer_data(&self, layer: u64) -> Vec<T> {
        self.layer_vertices(layer)
            .iter()
            .map(|vertex| vertex.data.clone())
            .collect()
    }
    /// Get a vertex by its ID
    pub fn get(&self, id: &str) -> Option<&Vertex<T>> {
        self.vertices.get(id)
    }
}

impl<T> Vertex<T>
where
    T: Hash + AsBytes + Clone,
{
    pub fn from_data(data: T, layer: u64, parents: HashSet<String>) -> Self {
        let id = data.digest().as_hex_string();
        Self::new(data, layer, parents, id)
    }
    /// Compute the id a vertex ID from data: Assume that all vertices have unique data. (headers have hash(round, author) since an authority can only produce one header per round)
    pub fn id_of(data: &T) -> String {
        data.digest().as_hex_string()
    }
}

impl From<Certificate> for Vertex<Certificate> {
    fn from(certificate: Certificate) -> Self {
        let layer = certificate.round();
        let parents = certificate.parents_as_hex();
        let id = certificate.digest().as_hex_string();
        Self::new(certificate, layer, parents, id)
    }
}
impl<T> AsBytes for Vertex<T>
where
    T: AsBytes + Clone,
{
    fn bytes(&self) -> Vec<u8> {
        self.data
            .bytes()
            .iter()
            .chain(self.layer.to_be_bytes().iter())
            .chain(self.parents.iter().flat_map(|parent| parent.as_bytes()))
            .copied()
            .collect()
    }
}

#[derive(thiserror::Error, Debug)]
pub enum DagError {
    #[error("Missing parents")]
    MissingParents(HashSet<String>),
}

#[cfg(test)]
mod test {
    use super::{Dag, Vertex};
    use crate::types::traits::{AsHex, Hash};
    use rstest::rstest;
    use std::collections::HashSet;

    #[rstest]
    fn test_one_child_one_parent_check() {
        let mut dag = Dag::new_with_root(0, 42);
        let child = Vertex::from_data(
            43,
            1,
            HashSet::from_iter([42.digest().as_hex_string()].into_iter()),
        );
        assert!(dag.insert_checked(child).is_ok());
    }
}
