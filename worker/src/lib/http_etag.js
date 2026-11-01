// Weak validators do not establish byte-for-byte identity for range reads.
export function strongEtag(headers) {
  const value = headers.get('ETag');
  return value && !value.startsWith('W/') ? value : undefined;
}

// PMTiles permits missing validators; readers that already received a strong
// validator from HEAD can require it on subsequent range responses.
export function etagMismatch(expected, received, { requireReceived = false } = {}) {
  if (!expected) return false;
  return received ? received !== expected : requireReceived;
}
