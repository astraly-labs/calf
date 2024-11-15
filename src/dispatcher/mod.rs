use crate::types::Transaction;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

pub struct Dispatcher {
    _workers: Vec<tokio::sync::mpsc::Sender<Transaction>>,
}

async fn handle_connection(
    mut socket: TcpStream,
    worker_connections: Vec<tokio::sync::mpsc::Sender<Vec<u8>>>,
) {
    let mut buffer = [0; 1024];
    let mut worker_chan = worker_connections.iter().cycle();
    loop {
        match socket.read(&mut buffer).await {
            Ok(0) => {
                println!("Client disconnected");
                break;
            }
            Ok(n) => {
                println!("Received: {}", String::from_utf8_lossy(&buffer[..n]));
                worker_chan
                    .next()
                    .unwrap()
                    .send(buffer[..n].to_vec())
                    .await
                    .unwrap();
                if let Err(e) = socket.write_all(&buffer[..n]).await {
                    eprintln!("Failed to write to socket: {}", e);
                    break;
                }
            }
            Err(e) => {
                eprintln!("Failed to read from socket: {}", e);
                break;
            }
        }
    }
}

impl Dispatcher {
    pub fn spawn(
        address: String,
        worker_connection: Vec<tokio::sync::mpsc::Sender<Vec<u8>>>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            if let Ok(listener) = TcpListener::bind(&address).await {
                println!("Server listening on {}", address);

                while let Ok((socket, addr)) = listener.accept().await {
                    println!("New connection from: {}", addr);
                    tokio::spawn(handle_connection(socket, worker_connection.clone()));
                }
            } else {
                eprintln!("Failed to bind to {}", address);
            }
        })
    }
}
