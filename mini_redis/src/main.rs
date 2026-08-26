mod server;
mod store;

use server::handle_client;
use store::Db;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() {
    let db = Db::new();
    db.spawn_purger();

    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    println!("Mini Redis server running on 127.0.0.1:6379...");

    loop {
        match listener.accept().await {
            Ok((socket, addr)) => {
                println!("New connection from {}", addr);

                let db_clone = db.clone();

                tokio::spawn(async move {
                    handle_client(socket, db_clone).await;
                });
            }
            Err(e) => println!("Failed to accept connection: {}", e),
        }
    }
}
