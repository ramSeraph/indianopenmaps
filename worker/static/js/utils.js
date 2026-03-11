export function proxyUrl(url) {
  return `/proxy?url=${encodeURIComponent(url)}`;
}
