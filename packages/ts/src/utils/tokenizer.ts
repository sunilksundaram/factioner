/**
 * Lightweight token estimator (no external vocab).
 * Heuristic: ~4 chars/token with a word-boundary floor so short words aren't undercounted.
 */
export function estimateTokens(text: string): number {
  if (!text) return 0;
  const byChars = Math.ceil(text.length / 4);
  const words = text.trim().length === 0 ? 0 : text.trim().split(/\s+/).length;
  return Math.max(1, byChars, words);
}

export function estimateTokensFromParts(parts: Array<string | null | undefined>): number {
  let total = 0;
  for (const p of parts) {
    if (p) total += estimateTokens(p);
  }
  return Math.max(total, parts.some(Boolean) ? 1 : 0);
}
