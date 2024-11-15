use crate::{types::Transaction, worker::Worker};

pub struct Dispatcher {
    workers : Vec<tokio::sync::mpsc::Sender<Transaction>>,
}

impl Dispatcher {
    pub fn spawn() {
    }
    
    pub fn run_forever() {
        // wait for new tx
        // dispatch 
    }
}