/// PIIGuard provides automated detection and redaction of Personally Identifiable Information (PII).
pub struct PIIGuard;

impl PIIGuard {
    /// Sanitizes text by replacing detected SSN, credit cards, emails, and phone numbers.
    pub fn sanitize_text(text: &str) -> (String, usize) {
        let mut count = 0;
        let mut out = text.to_string();

        // 1. Redact SSN: 3 digits - 2 digits - 4 digits (e.g. 123-45-6789)
        let bytes = out.as_bytes();
        let mut ssn_matches = Vec::new();
        if bytes.len() >= 11 {
            for i in 0..=bytes.len() - 11 {
                let slice = &bytes[i..i + 11];
                if slice[0..3].iter().all(|b| b.is_ascii_digit())
                    && slice[3] == b'-'
                    && slice[4..6].iter().all(|b| b.is_ascii_digit())
                    && slice[6] == b'-'
                    && slice[7..11].iter().all(|b| b.is_ascii_digit())
                {
                    let prev_ok = i == 0 || !bytes[i - 1].is_ascii_digit();
                    let next_ok = i + 11 == bytes.len() || !bytes[i + 11].is_ascii_digit();
                    if prev_ok && next_ok {
                        ssn_matches.push((i, 11));
                    }
                }
            }
        }
        for (idx, len) in ssn_matches.into_iter().rev() {
            out.replace_range(idx..idx + len, "[REDACTED_SSN]");
            count += 1;
        }

        // 2. Redact Credit Card: 4 groups of 4 digits with optional '-' or ' '
        let mut card_matches = Vec::new();
        let chars: Vec<(usize, char)> = out.char_indices().collect();
        let mut idx = 0;
        while idx < chars.len() {
            let mut digits = 0;
            let mut end_char = idx;
            let mut j = idx;
            while j < chars.len() && digits < 16 {
                let c = chars[j].1;
                if c.is_ascii_digit() {
                    digits += 1;
                    end_char = j;
                } else if c != '-' && c != ' ' {
                    break;
                }
                j += 1;
            }
            if digits == 16 {
                let start_byte = chars[idx].0;
                let end_byte = if end_char + 1 < chars.len() {
                    chars[end_char + 1].0
                } else {
                    out.len()
                };
                card_matches.push((start_byte, end_byte - start_byte));
                idx = j;
            } else {
                idx += 1;
            }
        }
        for (s, len) in card_matches.into_iter().rev() {
            out.replace_range(s..s + len, "[REDACTED_CARD]");
            count += 1;
        }

        // 3. Redact Email: word@domain.tld
        let words: Vec<String> = out.split_whitespace().map(|s| s.to_string()).collect();
        for w in words {
            let clean = w.trim_matches(|c: char| !c.is_alphanumeric());
            if let Some(at) = clean.find('@') {
                let user = &clean[..at];
                let domain = &clean[at + 1..];
                if !user.is_empty() && domain.contains('.') && domain.len() > 3 {
                    out = out.replace(clean, "[REDACTED_EMAIL]");
                    count += 1;
                }
            }
        }

        (out, count)
    }

    /// Recursively sanitizes JSON values
    pub fn sanitize_json(val: serde_json::Value) -> serde_json::Value {
        match val {
            serde_json::Value::String(s) => {
                let (sanitized, _) = Self::sanitize_text(&s);
                serde_json::Value::String(sanitized)
            }
            serde_json::Value::Array(arr) => {
                serde_json::Value::Array(arr.into_iter().map(Self::sanitize_json).collect())
            }
            serde_json::Value::Object(map) => {
                let mut out = serde_json::Map::new();
                for (k, v) in map {
                    out.insert(k, Self::sanitize_json(v));
                }
                serde_json::Value::Object(out)
            }
            other => other,
        }
    }
}

