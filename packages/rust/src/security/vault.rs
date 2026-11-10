use crate::types::SecretCredential;
use aes_gcm::aead::{Aead, KeyInit};
use aes_gcm::{Aes256Gcm, Nonce};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::RwLock;

struct StoredCredential {
    cred_type: String,
    ciphertext: Vec<u8>,
    nonce: [u8; 12],
    header_name: Option<String>,
    _masked: String,
    suffix: String,
}

pub struct CredentialVault {
    credentials: RwLock<HashMap<String, StoredCredential>>,
    cipher: Aes256Gcm,
}

fn poison_ok<T>(result: Result<T, std::sync::PoisonError<T>>) -> T {
    result.unwrap_or_else(|e| e.into_inner())
}

impl CredentialVault {
    pub fn new() -> Self {
        Self::with_key(None)
    }

    pub fn with_key(key: Option<&str>) -> Self {
        let key_bytes = match key {
            Some(k) => {
                let mut hasher = Sha256::new();
                hasher.update(k.as_bytes());
                hasher.finalize().to_vec()
            }
            None => {
                use rand::RngCore;
                let mut buf = [0u8; 32];
                rand::thread_rng().fill_bytes(&mut buf);
                buf.to_vec()
            }
        };
        let cipher = Aes256Gcm::new_from_slice(&key_bytes).expect("valid key length");
        Self {
            credentials: RwLock::new(HashMap::new()),
            cipher,
        }
    }

    pub fn register(&self, endpoint_id: &str, credential: SecretCredential) {
        let mut nonce_bytes = [0u8; 12];
        use rand::RngCore;
        rand::thread_rng().fill_bytes(&mut nonce_bytes);
        let nonce = Nonce::from_slice(&nonce_bytes);
        let ciphertext = self
            .cipher
            .encrypt(nonce, credential.value.as_bytes())
            .expect("encrypt");

        let masked = Self::mask_credential(&credential.value);
        let suffix = if credential.value.len() >= 4 {
            credential.value[credential.value.len() - 4..].to_string()
        } else {
            credential.value.clone()
        };
        let mut map = poison_ok(self.credentials.write());
        map.insert(
            endpoint_id.to_string(),
            StoredCredential {
                cred_type: credential.cred_type,
                ciphertext,
                nonce: nonce_bytes,
                header_name: credential.header_name,
                _masked: masked,
                suffix,
            },
        );
    }

    fn decrypt(&self, stored: &StoredCredential) -> String {
        let nonce = Nonce::from_slice(&stored.nonce);
        let plain = self
            .cipher
            .decrypt(nonce, stored.ciphertext.as_ref())
            .expect("decrypt");
        String::from_utf8(plain).unwrap_or_default()
    }

    pub fn get_auth_header(&self, endpoint_id: &str) -> Option<(String, String)> {
        let map = poison_ok(self.credentials.read());
        map.get(endpoint_id).map(|stored| {
            let value = self.decrypt(stored);
            let header_name = stored.header_name.clone().unwrap_or_else(|| {
                if stored.cred_type == "bearer" || stored.cred_type == "oauth2" {
                    "Authorization".to_string()
                } else {
                    "api-key".to_string()
                }
            });
            let val = if stored.cred_type == "bearer" || stored.cred_type == "oauth2" {
                if value.starts_with("Bearer ") {
                    value
                } else {
                    format!("Bearer {}", value)
                }
            } else {
                value
            };
            (header_name, val)
        })
    }

    pub fn mask_credential(secret: &str) -> String {
        if secret.len() <= 10 {
            "****".to_string()
        } else {
            format!("{}...{}", &secret[..2], &secret[secret.len() - 2..])
        }
    }

    pub fn redact_secrets(&self, input: &str) -> String {
        let mut sanitized = input.to_string();

        {
            let map = poison_ok(self.credentials.read());
            for stored in map.values() {
                if stored.suffix.len() >= 4 && !sanitized.contains(&stored.suffix) {
                    continue;
                }
                let raw = self.decrypt(stored);
                if raw.len() > 3 {
                    sanitized = sanitized.replace(&raw, "[REDACTED_SECRET]");
                }
            }
        }

        // Redact Bearer tokens (skip already-redacted placeholders)
        let mut search_from = 0usize;
        while let Some(rel) = sanitized[search_from..].find("Bearer ") {
            let pos = search_from + rel;
            let after_start = pos + 7;
            let after = &sanitized[after_start..];
            if after.starts_with("[REDACTED_TOKEN]") {
                search_from = after_start + 16;
                continue;
            }
            let token_end = after
                .find(|c: char| c.is_whitespace() || c == ',' || c == ';')
                .unwrap_or(after.len());
            if token_end >= 6 {
                sanitized = format!(
                    "{}Bearer [REDACTED_TOKEN]{}",
                    &sanitized[..pos],
                    &sanitized[after_start + token_end..]
                );
                search_from = pos + "Bearer [REDACTED_TOKEN]".len();
            } else {
                break;
            }
        }

        for key_prefix in &["api_key=", "api-key=", "password=", "secret="] {
            let mut search_from = 0usize;
            let prefix_len = key_prefix.len();
            while let Some(rel) = sanitized[search_from..].find(key_prefix) {
                let pos = search_from + rel;
                let after_start = pos + prefix_len;
                let after = &sanitized[after_start..];
                if after.starts_with("[REDACTED]") {
                    search_from = after_start + 10;
                    continue;
                }
                let val_end = after
                    .find(|c: char| c.is_whitespace() || c == ',' || c == ';')
                    .unwrap_or(after.len());
                if val_end == 0 {
                    break;
                }
                sanitized = format!(
                    "{}{}[REDACTED]{}",
                    &sanitized[..pos],
                    key_prefix,
                    &sanitized[after_start + val_end..]
                );
                search_from = pos + prefix_len + 10;
            }
        }

        sanitized
    }

    pub fn remove(&self, endpoint_id: &str) {
        let mut map = poison_ok(self.credentials.write());
        map.remove(endpoint_id);
    }
}

impl Default for CredentialVault {
    fn default() -> Self {
        Self::new()
    }
}
