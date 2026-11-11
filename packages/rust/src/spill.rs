use serde_json::{json, Value};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, SystemTime};
use uuid::Uuid;

pub const SPILL_URI_PREFIX: &str = "spill://";

static GLOBAL_SPILL: OnceLock<Arc<BlobSpillStore>> = OnceLock::new();

pub fn configure_global_spill(store: Arc<BlobSpillStore>) {
    let _ = GLOBAL_SPILL.set(store);
}

pub fn global_spill() -> Option<Arc<BlobSpillStore>> {
    GLOBAL_SPILL.get().cloned()
}

pub fn resolve_spilled_string(value: &str) -> Option<String> {
    if !BlobSpillStore::is_spill_uri(value) {
        return None;
    }
    global_spill()?.read_string(value).ok()
}

pub struct BlobSpillStore {
    dir: PathBuf,
    threshold_bytes: usize,
    ttl: Duration,
}

impl BlobSpillStore {
    pub fn new(dir: impl AsRef<Path>, threshold_bytes: Option<usize>, ttl_ms: Option<u64>) -> std::io::Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;
        Ok(Self {
            dir,
            threshold_bytes: threshold_bytes.unwrap_or(256 * 1024),
            ttl: Duration::from_millis(ttl_ms.unwrap_or(3_600_000)),
        })
    }

    pub fn threshold_bytes(&self) -> usize {
        self.threshold_bytes
    }

    pub fn is_spill_uri(value: &str) -> bool {
        value.starts_with(SPILL_URI_PREFIX)
    }

    pub fn put(&self, data: &[u8], ext: &str) -> std::io::Result<String> {
        self.cleanup_expired();
        let name = format!("{}.{}", Uuid::new_v4(), ext);
        let path = self.dir.join(&name);
        fs::write(&path, data)?;
        Ok(format!("{}{}", SPILL_URI_PREFIX, name))
    }

    fn resolve_path(&self, uri: &str) -> std::io::Result<PathBuf> {
        if !Self::is_spill_uri(uri) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "not a spill URI",
            ));
        }
        let name = &uri[SPILL_URI_PREFIX.len()..];
        if name.contains("..") || name.contains('/') || name.contains('\\') {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "invalid spill URI",
            ));
        }
        Ok(self.dir.join(name))
    }

    pub fn read_string(&self, uri: &str) -> std::io::Result<String> {
        fs::read_to_string(self.resolve_path(uri)?)
    }

    pub fn delete(&self, uri: &str) {
        if let Ok(p) = self.resolve_path(uri) {
            let _ = fs::remove_file(p);
        }
    }

    pub fn cleanup_expired(&self) -> usize {
        let Ok(entries) = fs::read_dir(&self.dir) else {
            return 0;
        };
        let now = SystemTime::now();
        let mut removed = 0usize;
        for entry in entries.flatten() {
            let Ok(meta) = entry.metadata() else { continue };
            let Ok(modified) = meta.modified() else { continue };
            if now.duration_since(modified).unwrap_or_default() > self.ttl {
                if fs::remove_file(entry.path()).is_ok() {
                    removed += 1;
                }
            }
        }
        removed
    }

    /// Spill large known JSON fields to `spill://` URIs.
    pub fn spill_value(&self, mut value: Value) -> Value {
        match &mut value {
            Value::Object(map) => {
                if let Some(Value::String(s)) = map.get("base64Source") {
                    if s.len() >= self.threshold_bytes {
                        if let Ok(uri) = self.put(s.as_bytes(), "b64") {
                            map.insert("base64Source".into(), json!(uri));
                        }
                    }
                }
                if let Some(Value::String(s)) = map.get("input") {
                    if s.len() >= self.threshold_bytes {
                        if let Ok(uri) = self.put(s.as_bytes(), "txt") {
                            map.insert("input".into(), json!(uri));
                        }
                    }
                } else if let Some(Value::Array(arr)) = map.get_mut("input") {
                    for item in arr.iter_mut() {
                        if let Value::String(s) = item {
                            if s.len() >= self.threshold_bytes {
                                if let Ok(uri) = self.put(s.as_bytes(), "txt") {
                                    *item = json!(uri);
                                }
                            }
                        }
                    }
                }
                if let Some(Value::Array(msgs)) = map.get_mut("messages") {
                    for msg in msgs.iter_mut() {
                        if let Value::Object(m) = msg {
                            if let Some(Value::String(s)) = m.get("content") {
                                if s.len() >= self.threshold_bytes {
                                    if let Ok(uri) = self.put(s.as_bytes(), "txt") {
                                        m.insert("content".into(), json!(uri));
                                    }
                                }
                            }
                        }
                    }
                }
            }
            _ => {}
        }
        value
    }

    pub fn resolve_value(&self, mut value: Value) -> Value {
        match &mut value {
            Value::Object(map) => {
                if let Some(Value::String(s)) = map.get("base64Source").cloned() {
                    if Self::is_spill_uri(&s) {
                        if let Ok(text) = self.read_string(&s) {
                            map.insert("base64Source".into(), json!(text));
                        }
                    }
                }
                if let Some(Value::String(s)) = map.get("input").cloned() {
                    if Self::is_spill_uri(&s) {
                        if let Ok(text) = self.read_string(&s) {
                            map.insert("input".into(), json!(text));
                        }
                    }
                } else if let Some(Value::Array(arr)) = map.get_mut("input") {
                    for item in arr.iter_mut() {
                        if let Value::String(s) = item {
                            if Self::is_spill_uri(s) {
                                if let Ok(text) = self.read_string(s) {
                                    *item = json!(text);
                                }
                            }
                        }
                    }
                }
                if let Some(Value::Array(msgs)) = map.get_mut("messages") {
                    for msg in msgs.iter_mut() {
                        if let Value::Object(m) = msg {
                            if let Some(Value::String(s)) = m.get("content").cloned() {
                                if Self::is_spill_uri(&s) {
                                    if let Ok(text) = self.read_string(&s) {
                                        m.insert("content".into(), json!(text));
                                    }
                                }
                            }
                        }
                    }
                }
            }
            _ => {}
        }
        value
    }

    pub fn collect_uris(value: &Value, into: &mut Vec<String>) {
        match value {
            Value::String(s) if Self::is_spill_uri(s) => into.push(s.clone()),
            Value::Array(arr) => {
                for v in arr {
                    Self::collect_uris(v, into);
                }
            }
            Value::Object(map) => {
                for v in map.values() {
                    Self::collect_uris(v, into);
                }
            }
            _ => {}
        }
    }

    pub fn delete_payload_spills(&self, value: &Value) {
        let mut uris = Vec::new();
        Self::collect_uris(value, &mut uris);
        for uri in uris {
            self.delete(&uri);
        }
    }
}
