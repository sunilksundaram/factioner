/** Convert Fetch/Undici Headers to a plain record without relying on `.entries()`. */
export function headersToRecord(headers: {
  forEach(callback: (value: string, key: string) => void): void;
}): Record<string, string> {
  const out: Record<string, string> = {};
  headers.forEach((value, key) => {
    out[key] = value;
  });
  return out;
}
