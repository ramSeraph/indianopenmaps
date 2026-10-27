export const metadata = { name: 'Retry fixture', format: 'mvt', vector_layers: [{ id: 'test' }] };
export const tile = new Uint8Array([0x1a, 0]);

// PMTiles v3 with one uncompressed z0 tile, optionally behind a leaf directory.
export function makeArchive({ leafDirectory = false } = {}) {
  const tileDirectory = new Uint8Array([1, 0, 1, tile.length, 1]);
  const directory = leafDirectory ? new Uint8Array([1, 0, 0, tileDirectory.length, 1]) : tileDirectory;
  const json = new TextEncoder().encode(JSON.stringify(metadata));
  const metadataOffset = 127 + directory.length;
  const leafOffset = leafDirectory ? 16384 : 0;
  const tileOffset = leafDirectory ? leafOffset + tileDirectory.length : metadataOffset + json.length;
  const bytes = new Uint8Array(tileOffset + tile.length);
  bytes.set(new TextEncoder().encode('PMTiles'));
  const view = new DataView(bytes.buffer);
  view.setUint8(7, 3);
  for (const [offset, value] of [
    [8, 127], [16, directory.length], [24, metadataOffset], [32, json.length],
    [40, leafOffset], [48, leafDirectory ? tileDirectory.length : 0],
    [56, tileOffset], [64, tile.length], [72, 1], [80, 1], [88, 1],
  ]) view.setBigUint64(offset, BigInt(value), true);
  for (const offset of [96, 97, 98, 99]) view.setUint8(offset, 1);
  for (const [offset, value] of [[102, -180], [106, -90], [110, 180], [114, 90]]) {
    view.setInt32(offset, value * 1e7, true);
  }
  bytes.set(directory, 127);
  bytes.set(json, metadataOffset);
  if (leafDirectory) bytes.set(tileDirectory, leafOffset);
  bytes.set(tile, tileOffset);
  return bytes;
}

export const archive = makeArchive();
const header = {
  min_zoom: 0, max_zoom: 0, tile_type: 1, tile_compression: 1,
  min_lon_e7: -180e7, min_lat_e7: -90e7, max_lon_e7: 180e7, max_lat_e7: 90e7,
  center_lon_e7: 0, center_lat_e7: 0, center_zoom: 0,
};
export const mosaic = { version: 1, header, metadata, slices: { 'part.pmtiles': { header } } };

export function respond(url, options, bytes = archive) {
  if (url.endsWith('.json')) return Response.json(mosaic);
  const [, start, end] = options.headers.get('Range').match(/bytes=(\d+)-(\d+)/);
  return new Response(bytes.slice(Number(start), Number(end) + 1), {
    status: 206, headers: { ETag: 'fixture' },
  });
}
