# Mini-Redis Clone (Rust)

A lightweight, high-performance, in-memory key-value data store written in Rust. 

This project implements a fully asynchronous TCP server using Tokio and natively supports the Redis Serialization Protocol (RESP). Because it speaks the official protocol, it can be accessed directly using standard Redis clients like `redis-cli`.

## Current Features

* **Asynchronous TCP Server**: Built on top of the Tokio runtime, capable of handling concurrent client connections efficiently.
* **RESP Protocol Support**: Native parsing and formatting of the Redis Serialization Protocol (binary-safe arrays and bulk strings).
* **Core Data Operations**: O(1) time complexity for `SET`, `GET`, and `DEL` commands.
* **Time-To-Live (TTL)**: Support for the `EXPIRE` command.
* **Active Memory Management**: A dedicated background task that continuously sweeps the database to purge expired keys and prevent memory leaks.
* **Disk Persistence**: Includes a `SAVE` command that securely calculates remaining TTLs and serializes the in-memory state to a `dump.json` file for recovery across server restarts.

## Getting Started

### Prerequisites
* [Rust and Cargo](https://rustup.rs/)
* `redis-cli` (optional, for connecting to the server)

### Installation & Running
1. Clone the repository:
   ```bash
   git clone [https://github.com/YOUR_USERNAME/mini_redis.git](https://github.com/YOUR_USERNAME/mini_redis.git)
   cd mini_redis
   ```
2. Build and run the server (using release mode for optimal performance):
   ```bash
   cargo run --release
   ```
   The server will start listening on `127.0.0.1:6379`.

### Usage

Open a new terminal window and connect using the standard Redis CLI:

```bash
redis-cli
```

Test the core commands:
```text
127.0.0.1:6379> SET vehicle truck
+OK
127.0.0.1:6379> GET vehicle
"truck"
127.0.0.1:6379> EXPIRE vehicle 10
:1
127.0.0.1:6379> SAVE
+OK
```

*(Alternatively, you can connect using raw TCP via `nc 127.0.0.1 6379` or `telnet 127.0.0.1 6379`)*.

## Roadmap & Planned Architecture Improvements

While the current version is fully functional, I am actively planning the following system architecture upgrades to make the store production-ready:

* **Append-Only File (AOF)**: Transitioning from JSON snapshots to a Write-Ahead Log. This will ensure that every mutating operation is immediately appended to disk, providing crash-resistant durability without losing data between `SAVE` commands.
* **Sharded Concurrency**: Replacing the single global `RwLock<HashMap>` with a sharded map architecture (e.g., using concepts similar to `dashmap`). This will drastically reduce lock contention when thousands of concurrent clients attempt to write data simultaneously.
* **LRU Eviction Policy (Least Recently Used)**: Implementing a memory-bound eviction algorithm to automatically delete the least recently accessed keys when the server reaches its maximum memory capacity.