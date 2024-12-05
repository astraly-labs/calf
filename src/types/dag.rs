use derive_more::derive::Constructor;
use getset::CopyGetters;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

use super::certificate::Certificate;
use super::traits::{AsBytes, AsHex, Hash};

#[derive(Serialize, Deserialize, Debug, Clone, CopyGetters)]
/// A particular type of DAG where vertices are ordonned by layers, vertices from each layer can only have parents from previous layers and the data of each vertex is unique.
pub struct Dag<T>
where
    T: Hash + AsBytes + Clone,
{
    vertices: HashMap<String, Vertex<T>>,
    vertices_by_layers: HashMap<u32, HashSet<String>>,
    #[getset(get_copy = "pub")]
    height: u32,
    #[getset(get_copy = "pub")]
    base_layer: u32,
}

#[derive(Serialize, Deserialize, Debug, Clone, Constructor)]
/// A particular type of vertex for a particular type of DAG.
pub struct Vertex<T>
where
    T: Hash + AsBytes + Clone,
{
    data: T,
    layer: u32,
    parents: HashSet<String>,
    id: String,
}

impl<T> Dag<T>
where
    T: Hash + AsBytes + Clone,
{
    /// Create a new DAG with a base layer value (can be anything).
    pub fn new(base_layer: u32) -> Self {
        Self {
            vertices: HashMap::new(),
            vertices_by_layers: HashMap::new(),
            height: base_layer,
            base_layer,
        }
    }
    pub fn new_with_root(base_layer: u32, root: T) -> Self {
        let vertex = Vertex::from_data(root, base_layer, HashSet::new());
        let mut dag = Self::new(base_layer);
        dag.insert(vertex).unwrap();
        dag
    }
    /// Check if a vertex has all its parents in the DAG, returning an error containing the missing parents if not.
    pub fn check_parents(&self, vertex: &Vertex<T>) -> Result<(), DagError> {
        if vertex.layer == 0 {
            Ok(())
        } else {
            self.vertices_by_layers
                .get(&(vertex.layer - 1))
                .map(|potential_parents| {
                    if vertex.parents.is_subset(potential_parents) {
                        Ok(())
                    } else {
                        Err(DagError::MissingParents(
                            vertex
                                .parents
                                .difference(potential_parents)
                                .map(|elm| elm.clone())
                                .collect(),
                        ))
                    }
                })
                .unwrap_or(Err(DagError::MissingParents(vertex.parents.clone())))
        }
    }
    /// Insert a vertex in the DAG, returning an error if its parents are missing but inserting it anyway.
    pub fn insert(&mut self, vertex: Vertex<T>) -> Result<(), DagError> {
        let res = self.check_parents(&vertex);
        let id = vertex.id.clone();
        let layer = vertex.layer.clone();
        self.vertices.insert(id.clone(), vertex);
        self.vertices_by_layers
            .entry(layer)
            .or_insert(HashSet::new())
            .insert(id);
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
    pub fn layer_vertices(&self, layer: u32) -> Vec<&Vertex<T>> {
        self.vertices_by_layers
            .get(&layer)
            .map(|keys| keys.iter().flat_map(|key| self.vertices.get(key)).collect())
            .unwrap_or(Vec::new())
    }
    /// Get the number of vertices belonging to a given layer.
    pub fn layer_size(&self, layer: u32) -> usize {
        self.vertices_by_layers
            .get(&layer)
            .map(|keys| keys.len())
            .unwrap_or(0)
    }
    /// Get the cloned data of all vertices for a given layer.
    pub fn layer_data(&self, layer: u32) -> Vec<T> {
        self.layer_vertices(layer)
            .iter()
            .map(|vertex| vertex.data.clone())
            .collect()
    }
}

impl<T> Vertex<T>
where
    T: Hash + AsBytes + Clone,
{
    pub fn from_data(data: T, layer: u32, parents: HashSet<String>) -> Self {
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
        Self::new(certificate, layer as u32, parents, id)
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
    use std::collections::HashSet;

    use rstest::rstest;

    use crate::types::traits::{AsHex, Hash};

    use super::{Dag, Vertex};

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
