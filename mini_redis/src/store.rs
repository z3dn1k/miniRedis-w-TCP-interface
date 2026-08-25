use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Write};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub struct Entry {
    pub value: String,
    pub expires_at: Option<Instant>,
}

impl Entry {
    pub fn is_expired(&self) -> bool {
        match self.expires_at {
            Some(time) => Instant::now() > time,
            None => false,
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct DiskEntry {
    pub value: String,
    pub ttl_millis: Option<u64>,
}

#[derive(Clone)]
pub struct Db {
    state: Arc<RwLock<HashMap<String, Entry>>>,
}

impl Db {
    pub fn new() -> Self {
        let mut map = HashMap::new();

        if let Ok(mut file) = File::open("dump.json") {
            let mut json = String::new();
            if file.read_to_string(&mut json).is_ok() {
                if let Ok(disk_map) = serde_json::from_str::<HashMap<String, DiskEntry>>(&json) {
                    let now = Instant::now();

                    for (k, v) in disk_map {
                        let expires_at = v.ttl_millis.map(|ms| now + Duration::from_millis(ms));
                        map.insert(
                            k,
                            Entry {
                                value: v.value,
                                expires_at,
                            },
                        );
                    }
                    println!("Successfully loaded data from dump.json!");
                }
            }
        }

        Db {
            state: Arc::new(RwLock::new(map)),
        }
    }

    pub fn save(&self) -> Result<(), Box<dyn std::error::Error>> {
        let map = self.state.read().unwrap();
        let mut disk_map: HashMap<String, DiskEntry> = HashMap::new();
        let now = Instant::now();

        for (k, v) in map.iter() {
            if !v.is_expired() {
                let ttl_millis = v.expires_at.map(|expire| {
                    expire.saturating_duration_since(now).as_millis() as u64
                });

                disk_map.insert(
                    k.clone(),
                    DiskEntry {
                        value: v.value.clone(),
                        ttl_millis,
                    },
                );
            }
        }

        let json = serde_json::to_string(&disk_map)?;
        let mut file = File::create("dump.json")?;
        file.write_all(json.as_bytes())?;

        Ok(())
    }

    pub fn set(&self, key: String, value: String, ttl: Option<Duration>) {
        let mut map = self.state.write().unwrap();
        let expires_at = ttl.map(|duration| Instant::now() + duration);
        map.insert(key, Entry { value, expires_at });
    }

    pub fn get(&self, key: &str) -> Option<String> {
        let map = self.state.read().unwrap();

        if let Some(entry) = map.get(key) {
            if entry.is_expired() {
                return None;
            }
            return Some(entry.value.clone());
        }
        None
    }

    pub fn del(&self, key: &str) -> bool {
        let mut map = self.state.write().unwrap();
        map.remove(key).is_some()
    }

    // NEW: EXPIRE method updates the TTL of an existing key
    pub fn expire(&self, key: &str, secs: u64) -> bool {
        let mut map = self.state.write().unwrap();

        if let Some(entry) = map.get_mut(key) {
            if entry.is_expired() {
                return false; // Act like it doesn't exist
            }
            entry.expires_at = Some(Instant::now() + Duration::from_secs(secs));
            return true;
        }
        false
    }

    pub fn spawn_purger(&self) {
        let state_clone = self.state.clone();

        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(1)).await;
                let mut map = state_clone.write().unwrap();
                map.retain(|_key, entry| !entry.is_expired());
            }
        });
    }
}