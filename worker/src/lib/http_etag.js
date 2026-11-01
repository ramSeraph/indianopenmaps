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

// GET/HEAD validators use weak comparison. Quoted opaque tags may contain commas.
export function ifNoneMatchMatches(value, etag) {
  if (!value) return false;
  if (value.trim() === '*') return true;
  const opaque = etag.replace(/^W\//, '');
  let remaining = value.trim();
  let matched = false;
  while (remaining) {
    const tag = /^(?:W\/)?("[^"\x00-\x20\x7f]*")\s*(,|$)/.exec(remaining);
    if (!tag) return false;
    matched ||= tag[1] === opaque;
    remaining = remaining.slice(tag[0].length).trimStart();
  }
  return matched;
}
