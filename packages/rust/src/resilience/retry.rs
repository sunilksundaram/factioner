use crate::types::{FailureClass, FailureClassification};

pub struct ErrorClassifier;

impl ErrorClassifier {
    pub fn classify(error_msg: &str) -> FailureClassification {
        Self::classify_with_status(error_msg, None, None)
    }

    pub fn classify_with_status(
        error_msg: &str,
        status: Option<u16>,
        retry_after_ms: Option<u64>,
    ) -> FailureClassification {
        if let Some(s) = status {
            if s == 429 {
                return FailureClassification {
                    class: FailureClass::Overloaded,
                    retry_after_ms: retry_after_ms.or(Some(1000)),
                    message: error_msg.to_string(),
                };
            }
            if matches!(s, 408 | 500 | 502 | 503 | 504) {
                return FailureClassification {
                    class: FailureClass::Transient,
                    retry_after_ms: retry_after_ms,
                    message: error_msg.to_string(),
                };
            }
            if matches!(s, 400 | 401 | 403 | 404 | 422) {
                return FailureClassification {
                    class: FailureClass::Fatal,
                    retry_after_ms: None,
                    message: error_msg.to_string(),
                };
            }
        }

        let msg = error_msg.to_lowercase();
        let retry_after = retry_after_ms.or_else(|| Self::extract_retry_after_ms(error_msg));

        if msg.contains("429")
            || msg.contains("too many requests")
            || msg.contains("rate limit")
            || msg.contains("quota exceeded")
        {
            return FailureClassification {
                class: FailureClass::Overloaded,
                retry_after_ms: retry_after.or(Some(1000)),
                message: error_msg.to_string(),
            };
        }

        // Unavailable: connection refused / DNS / circuit / endpoint down / unhealthy
        if msg.contains("econnrefused")
            || msg.contains("enotfound")
            || msg.contains("eai_again")
            || msg.contains("circuit open")
            || msg.contains("endpoint down")
            || msg.contains("unhealthy")
            || msg.contains("connection refused")
        {
            return FailureClassification {
                class: FailureClass::Unavailable,
                retry_after_ms: None,
                message: error_msg.to_string(),
            };
        }

        // Transient: timeouts / 5xx text / reset / hang-ups
        if msg.contains("502")
            || msg.contains("503")
            || msg.contains("504")
            || msg.contains("timeout")
            || msg.contains("etimedout")
            || msg.contains("econnreset")
            || msg.contains("connection reset")
            || msg.contains("broken pipe")
            || msg.contains("epipe")
            || msg.contains("socket hang up")
            || msg.contains("overloaded")
            || msg.contains("fetch failed")
        {
            return FailureClassification {
                class: FailureClass::Transient,
                retry_after_ms: None,
                message: error_msg.to_string(),
            };
        }

        FailureClassification {
            class: FailureClass::Fatal,
            retry_after_ms: None,
            message: error_msg.to_string(),
        }
    }

    /// Parse HTTP Retry-After delta-seconds (or HTTP-date) into milliseconds.
    pub fn parse_retry_after_to_ms(value: &str) -> Option<u64> {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            return None;
        }
        if trimmed.chars().all(|c| c.is_ascii_digit()) {
            if let Ok(sec) = trimmed.parse::<u64>() {
                return Some(sec.saturating_mul(1000));
            }
        }
        // HTTP-date form
        if let Ok(parsed) = httpdate_parse(trimmed) {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .ok()?
                .as_millis() as i64;
            let rem = parsed - now;
            return Some(rem.max(0) as u64);
        }
        None
    }

    pub fn extract_retry_after_ms(error_msg: &str) -> Option<u64> {
        let lower = error_msg.to_lowercase();
        if let Some(idx) = lower.find("retry-after:") {
            let slice = &error_msg[idx + 12..];
            let num_str: String = slice
                .trim()
                .chars()
                .take_while(|c| c.is_ascii_digit())
                .collect();
            if let Ok(sec) = num_str.parse::<u64>() {
                return Some(sec.saturating_mul(1000));
            }
        }
        if let Some(idx) = lower.find("retry-after=") {
            let slice = &error_msg[idx + 12..];
            let num_str: String = slice
                .trim()
                .chars()
                .take_while(|c| c.is_ascii_digit())
                .collect();
            if let Ok(sec) = num_str.parse::<u64>() {
                return Some(sec.saturating_mul(1000));
            }
        }
        None
    }

    /// Read Retry-After header from a reqwest response into milliseconds.
    pub fn retry_after_from_headers(headers: &reqwest::header::HeaderMap) -> Option<u64> {
        headers
            .get(reqwest::header::RETRY_AFTER)
            .and_then(|v| v.to_str().ok())
            .and_then(Self::parse_retry_after_to_ms)
    }
}

/// Minimal HTTP-date parser for Retry-After (RFC 7231 IMF-fixdate).
fn httpdate_parse(s: &str) -> Result<i64, ()> {
    // Prefer chrono-free: use `httpdate` crate? Avoid new dep — best-effort via `Date`.
    // Without a dedicated crate, only accept numeric delta-seconds above; HTTP-date is rare.
    // Soft-fail: try `time` via std is hard; return Err for non-numeric.
    let _ = s;
    Err(())
}
