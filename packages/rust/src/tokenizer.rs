/// Lightweight token estimator (no external vocab).
/// Heuristic: ~4 chars/token with a word-boundary floor.
pub fn estimate_tokens(text: &str) -> usize {
    if text.is_empty() {
        return 0;
    }
    let by_chars = (text.len() as f64 / 4.0).ceil() as usize;
    let words = text.split_whitespace().count();
    by_chars.max(words).max(1)
}

pub fn estimate_tokens_from_parts<'a, I>(parts: I) -> usize
where
    I: IntoIterator<Item = &'a str>,
{
    let mut total = 0usize;
    let mut any = false;
    for p in parts {
        if !p.is_empty() {
            any = true;
            total += estimate_tokens(p);
        }
    }
    if any {
        total.max(1)
    } else {
        0
    }
}
