use std::collections::HashSet;

use crate::types::{
    dag::{Dag, DagError, Vertex},
    traits::{AsBytes, Hash},
    Digest,
};

#[derive(Clone, Debug)]
struct TestData {
    value: u64,
}

impl AsBytes for TestData {
    fn bytes(&self) -> Vec<u8> {
        self.value.to_be_bytes().to_vec()
    }
}

#[tokio::test]
async fn test_dag_creation_and_basic_ops() {
    let base_layer: u64 = 0;
    let mut dag: Dag<TestData> = Dag::new(base_layer);

    // Test initial state
    assert_eq!(dag.height(), base_layer);
    assert_eq!(dag.base_layer(), base_layer);

    // Test vertex insertion
    let data = TestData { value: 1 };
    let parents = HashSet::new();
    let vertex = Vertex::from_data(data, 1, parents);
    let vertex_id = vertex.id().clone();

    dag.insert(vertex).unwrap();
    assert_eq!(dag.height(), 1);
    assert_eq!(dag.layer_size(1), 1);

    // Test vertex retrieval
    let retrieved = dag.get(&vertex_id).unwrap();
    assert_eq!(retrieved.data().value, 1);
    assert_eq!(*retrieved.layer(), 1);
}

#[tokio::test]
async fn test_dag_parent_child_relationships() {
    let mut dag: Dag<TestData> = Dag::new(0);

    // Create parent vertex
    let parent_data = TestData { value: 1 };
    let parent_vertex = Vertex::from_data(parent_data, 1, HashSet::new());
    let parent_id = parent_vertex.id().clone();
    dag.insert(parent_vertex).unwrap();

    // Create child vertex with parent reference
    let mut parents = HashSet::new();
    parents.insert(parent_id);
    let child_data = TestData { value: 2 };
    let child_vertex = Vertex::from_data(child_data, 2, parents);

    dag.insert_checked(child_vertex).unwrap();
}

#[tokio::test]
async fn test_dag_invalid_parent() {
    let mut dag: Dag<TestData> = Dag::new(0);

    let mut parents = HashSet::new();
    parents.insert("non_existent_parent".to_string());
    let data = TestData { value: 1 };
    let vertex = Vertex::from_data(data, 1, parents);

    match dag.insert_checked(vertex) {
        Err(DagError::MissingParents(_)) => (),
        _ => panic!("Expected MissingParents error"),
    }
}

#[tokio::test]
async fn test_dag_layer_operations() {
    let mut dag: Dag<TestData> = Dag::new(0);

    // Insert vertices in different layers
    for i in 1..=3 {
        let data = TestData { value: i };
        let vertex = Vertex::from_data(data, i as u64, HashSet::new());
        dag.insert(vertex).unwrap();
    }

    // Test layer queries
    assert_eq!(dag.layer_size(1), 1);
    assert_eq!(dag.layer_size(2), 1);
    assert_eq!(dag.layer_size(3), 1);

    let layer_2_vertices = dag.layer_vertices(2);
    assert_eq!(layer_2_vertices.len(), 1);
    assert_eq!(layer_2_vertices[0].data().value, 2);
}

#[tokio::test]
async fn test_dag_multiple_parents() {
    let mut dag: Dag<TestData> = Dag::new(0);

    // Create two parent vertices
    let parent1_data = TestData { value: 1 };
    let parent2_data = TestData { value: 2 };
    let parent1_vertex = Vertex::from_data(parent1_data, 1, HashSet::new());
    let parent2_vertex = Vertex::from_data(parent2_data, 1, HashSet::new());

    let parent1_id = parent1_vertex.id().clone();
    let parent2_id = parent2_vertex.id().clone();

    dag.insert(parent1_vertex).unwrap();
    dag.insert(parent2_vertex).unwrap();

    // Create child with multiple parents
    let mut parents = HashSet::new();
    parents.insert(parent1_id);
    parents.insert(parent2_id);

    let child_data = TestData { value: 3 };
    let child_vertex = Vertex::from_data(child_data, 2, parents);

    dag.insert_checked(child_vertex).unwrap();
    assert_eq!(dag.layer_size(2), 1);
}

#[tokio::test]
async fn test_dag_cyclic_insertion_prevention() {
    let mut dag: Dag<TestData> = Dag::new(0);

    // Create first vertex
    let data1 = TestData { value: 1 };
    let vertex1 = Vertex::from_data(data1, 1, HashSet::new());
    let vertex1_id = vertex1.id().clone();
    dag.insert(vertex1).unwrap();

    // Try to create a vertex in a lower layer referencing a higher layer
    let mut parents = HashSet::new();
    parents.insert(vertex1_id);
    let data2 = TestData { value: 2 };
    let vertex2 = Vertex::from_data(data2, 0, parents);

    assert!(dag.insert_checked(vertex2).is_err());
}

#[tokio::test]
async fn test_dag_complex_hierarchy() {
    let mut dag: Dag<TestData> = Dag::new(0);

    // Layer 1: Two vertices
    let vertex1_1 = Vertex::from_data(TestData { value: 11 }, 1, HashSet::new());
    let vertex1_2 = Vertex::from_data(TestData { value: 12 }, 1, HashSet::new());
    let id1_1 = vertex1_1.id().clone();
    let id1_2 = vertex1_2.id().clone();

    dag.insert(vertex1_1).unwrap();
    dag.insert(vertex1_2).unwrap();

    // Layer 2: Two vertices, each with one parent
    let mut parents2_1 = HashSet::new();
    parents2_1.insert(id1_1.clone());
    let mut parents2_2 = HashSet::new();
    parents2_2.insert(id1_2.clone());

    let vertex2_1 = Vertex::from_data(TestData { value: 21 }, 2, parents2_1);
    let vertex2_2 = Vertex::from_data(TestData { value: 22 }, 2, parents2_2);
    let id2_1 = vertex2_1.id().clone();
    let id2_2 = vertex2_2.id().clone();

    dag.insert_checked(vertex2_1).unwrap();
    dag.insert_checked(vertex2_2).unwrap();

    // Layer 3: One vertex with both layer 2 vertices as parents
    let mut parents3 = HashSet::new();
    parents3.insert(id2_1);
    parents3.insert(id2_2);

    let vertex3 = Vertex::from_data(TestData { value: 31 }, 3, parents3);
    dag.insert_checked(vertex3).unwrap();

    // Verify the structure
    assert_eq!(dag.layer_size(1), 2);
    assert_eq!(dag.layer_size(2), 2);
    assert_eq!(dag.layer_size(3), 1);
    assert_eq!(dag.height(), 3);
}
