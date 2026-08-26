# Mini-Redis Clone (Rust)

A lightweight, high-performance, in-memory key-value data store written in Rust. 

This project implements a fully asynchronous TCP server using Tokio and natively supports the Redis Serialization Protocol (RESP). Because it speaks the official protocol, it can be accessed directly using standard Redis clients like `redis-cli`.

## Architecture Highlights

This project was built with production-grade backend engineering patterns:

* **Sharded Concurrency (`DashMap`)**: Avoids global lock contention by dividing the key-value store into multiple independently locked shards, allowing thousands of concurrent clients to read and write simultaneously.
* **Non-Blocking AOF Persistence**: Utilizes Tokio's MPSC channels to offload file I/O. Mutating commands are instantly sent to a background task that continuously appends them to a Write-Ahead Log (`appendonly.aof`), guaranteeing data durability without blocking the main event loop.
* **Approximated LRU Eviction**: Implements a memory-bound cache eviction policy (max 10,000 keys). It uses random sampling and lock-free read tracking via `AtomicU64`, ensuring that `GET` operations remain strictly O(1) and require zero write-locks.
* **Continuous Integration**: Automated GitHub Actions pipeline that enforces code formatting, strict linting (`cargo clippy -D warnings`), and executes tests on every push.

## Core Features

* **Asynchronous TCP Server**: Built on top of the Tokio runtime.
* **RESP Protocol Support**: Native parsing and formatting of binary-safe arrays and bulk strings.
* **Core Commands**: O(1) time complexity for `SET`, `GET`, and `DEL`.
* **Time-To-Live (TTL)**: Support for the `EXPIRE` command, backed by an active sweeper task that periodically purges expired keys to prevent memory leaks.
* **Crash Recovery**: Automatically replays the `.aof` transaction log on startup to reconstruct the database state.

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
127.0.0.1:6379> DEL vehicle
:1
```

*(Alternatively, you can connect using raw TCP via `nc 127.0.0.1 6379` or `telnet 127.0.0.1 6379`)*.

## Roadmap

Future planned improvements:
* **Dockerization**: Provide a minimal `Dockerfile` for universal, environment-agnostic deployment.
* **Expanded Command Set**: Support for list operations (`LPUSH`, `RPOP`), `PING`, and `INCR`.
* **Benchmarking**: Publish automated throughput benchmarks using the official `redis-benchmark` tool.