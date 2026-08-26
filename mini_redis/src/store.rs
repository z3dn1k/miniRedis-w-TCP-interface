use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;

// We set a hard limit for our cache size.
// Once we exceed this, the LRU eviction kicks in.
const MAX_KEYS: usize = 10_000;
const EVICTION_SAMPLE_SIZE: usize = 5;

// Helper function to get current time as a simple integer
fn current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

// ----------------------------------------
// Data Structures
// ----------------------------------------

#[derive(Debug)]
pub struct Entry {
    pub value: String,
    pub expires_at: Option<Instant>,
    // Atomic allows us to update the timestamp on GET without write-locking!
    pub last_accessed: AtomicU64,
}

impl Entry {
    pub fn is_expired(&self) -> bool {
        match self.expires_at {
            Some(time) => Instant::now() > time,
            None => false,
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub enum AofCommand {
    Set { key: String, value: String },
    Del { key: String },
    Expire { key: String, secs: u64 },
}

#[derive(Clone)]
pub struct Db {
    state: Arc<DashMap<String, Entry>>,
    aof_tx: mpsc::UnboundedSender<String>,
}

// ----------------------------------------
// Database Implementation
// ----------------------------------------

impl Db {
    pub fn new() -> Self {
        let map = Arc::new(DashMap::new());

        if let Ok(file) = std::fs::File::open("appendonly.aof") {
            let reader = std::io::BufReader::new(file);
            use std::io::BufRead;

            // Notice we name the variable `line_str` here to match the code inside
            for line_str in reader.lines().map_while(Result::ok) {
                if let Ok(cmd) = serde_json::from_str::<AofCommand>(&line_str) {
                    match cmd {
                        AofCommand::Set { key, value } => {
                            map.insert(
                                key,
                                Entry {
                                    value,
                                    expires_at: None,
                                    last_accessed: AtomicU64::new(current_timestamp()),
                                },
                            );
                        }
                        AofCommand::Del { key } => {
                            map.remove(&key);
                        }
                        AofCommand::Expire { key, secs } => {
                            if let Some(mut entry) = map.get_mut(&key) {
                                entry.expires_at =
                                    Some(Instant::now() + Duration::from_secs(secs));
                            }
                        }
                    }
                }
            }
            println!("Successfully replayed appendonly.aof!");
        }

        let (tx, mut rx) = mpsc::unbounded_channel::<String>();

        tokio::spawn(async move {
            use tokio::io::AsyncWriteExt;
            let mut file = tokio::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open("appendonly.aof")
                .await
                .expect("Failed to open AOF file");

            while let Some(log_line) = rx.recv().await {
                let _ = file.write_all(log_line.as_bytes()).await;
                let _ = file.write_all(b"\n").await;
                let _ = file.flush().await;
            }
        });

        Db {
            state: map,
            aof_tx: tx,
        }
    }

    fn log_command(&self, cmd: AofCommand) {
        if let Ok(json) = serde_json::to_string(&cmd) {
            let _ = self.aof_tx.send(json);
        }
    }

    // ----------------------------------------
    // LRU Eviction Logic
    // ----------------------------------------
    fn evict_lru_if_needed(&self) {
        if self.state.len() < MAX_KEYS {
            return;
        }

        let mut oldest_key = None;
        let mut oldest_time = u64::MAX;

        // Take a small random sample of keys (DashMap iteration is pseudorandom enough)
        for entry in self.state.iter().take(EVICTION_SAMPLE_SIZE) {
            let time = entry.value().last_accessed.load(Ordering::Relaxed);
            if time < oldest_time {
                oldest_time = time;
                oldest_key = Some(entry.key().clone());
            }
        }

        // Remove the oldest key found in the sample
        if let Some(key) = oldest_key {
            self.state.remove(&key);
            self.log_command(AofCommand::Del { key });
        }
    }

    pub fn set(&self, key: String, value: String, ttl: Option<Duration>) {
        // Run eviction check before inserting new data
        self.evict_lru_if_needed();

        let expires_at = ttl.map(|duration| Instant::now() + duration);

        self.state.insert(
            key.clone(),
            Entry {
                value: value.clone(),
                expires_at,
                last_accessed: AtomicU64::new(current_timestamp()),
            },
        );
        self.log_command(AofCommand::Set { key, value });
    }

    pub fn get(&self, key: &str) -> Option<String> {
        if let Some(entry) = self.state.get(key) {
            if entry.is_expired() {
                return None;
            }

            // LOCK-FREE UPDATE!
            // We record that this key was just used without locking the map.
            entry
                .value()
                .last_accessed
                .store(current_timestamp(), Ordering::Relaxed);

            return Some(entry.value().value.clone());
        }
        None
    }

    pub fn del(&self, key: &str) -> bool {
        let deleted = self.state.remove(key).is_some();
        if deleted {
            self.log_command(AofCommand::Del {
                key: key.to_string(),
            });
        }
        deleted
    }

    pub fn expire(&self, key: &str, secs: u64) -> bool {
        if let Some(mut entry) = self.state.get_mut(key) {
            if entry.is_expired() {
                return false;
            }
            entry.expires_at = Some(Instant::now() + Duration::from_secs(secs));

            // Updating TTL counts as usage
            entry
                .last_accessed
                .store(current_timestamp(), Ordering::Relaxed);

            self.log_command(AofCommand::Expire {
                key: key.to_string(),
                secs,
            });
            return true;
        }
        false
    }

    pub fn spawn_purger(&self) {
        let state_clone = self.state.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(1)).await;
                state_clone.retain(|_key, entry| !entry.is_expired());
            }
        });
    }
}
