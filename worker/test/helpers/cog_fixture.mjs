import { deflateSync } from 'node:zlib';
import { PhotonImage } from '@cf-wasm/photon';

// Small classic TIFFs generated entirely in memory: full-resolution red data,
// a blue overview, and optional 1-bit masks. Distinct colors detect accidental
// selection of the primary image or a mask IFD at overview zooms.
export const TILE = { z: 10, x: 710, y: 446 };
export function makeCOG({ mask = true, jpeg = false, epsg = 3857, width = 512, height = 512, blockAlignment = 8 } = {}) {
  const block = 256;
  const span = 2 * Math.PI * 6378137 / 2 ** TILE.z;
  const resolution = span / 512;
  const west = -Math.PI * 6378137 + TILE.x * span;
  const north = Math.PI * 6378137 - TILE.y * span;
  const levels = [];
  for (const factor of [1, 2]) {
    const w = Math.ceil(width / factor), h = Math.ceil(height / factor);
    const color = factor === 1 ? [200, 60, 20] : [20, 60, 200];
    for (const isMask of mask ? [false, true] : [false]) {
      const data = [];
      for (let y = 0; y < Math.ceil(h / block); y++) {
        for (let x = 0; x < Math.ceil(w / block); x++) {
          if (isMask) {
            const bits = Buffer.alloc(block * block / 8);
            for (let row = 0; row < block; row++) {
              for (let col = 0; col < block; col++) {
                if (x * block + col >= 128 / factor) {
                  const pixel = row * block + col;
                  bits[pixel >> 3] |= 1 << (7 - (pixel & 7));
                }
              }
            }
            data.push(deflateSync(bits));
          } else if (jpeg) {
            const rgba = new Uint8Array(block * block * 4);
            for (let pixel = 0; pixel < block * block; pixel++) rgba.set([...color, 255], pixel * 4);
            const image = new PhotonImage(rgba, block, block);
            try { data.push(Buffer.from(image.get_bytes_jpeg(95))); }
            finally { image.free(); }
          } else {
            const rgb = Buffer.alloc(block * block * 3);
            for (let pixel = 0; pixel < block * block; pixel++) rgb.set(color, pixel * 3);
            data.push(deflateSync(rgb));
          }
        }
      }
      const tags = [
        [254, 4, [(factor === 1 ? 0 : 1) | (isMask ? 4 : 0)]],
        [256, 4, [w]], [257, 4, [h]], [258, 3, isMask ? [1] : [8, 8, 8]],
        [259, 3, [isMask || !jpeg ? 8 : 7]], [262, 3, [isMask ? 4 : jpeg ? 6 : 2]],
        [277, 3, [isMask ? 1 : 3]], [284, 3, [1]], [317, 3, [1]],
        [322, 4, [block]], [323, 4, [block]],
        [324, 4, data.map(() => 0)], [325, 4, data.map(b => b.length)], [339, 3, [1]],
      ];
      if (!levels.length) tags.push(
        [33550, 12, [resolution, resolution, 0]],
        [33922, 12, [0, 0, 0, west, north, 0]],
        [34735, 3, [1, 1, 0, 3, 1024, 0, 1, 1, 1025, 0, 1, 1, 3072, 0, 1, epsg]],
        [42112, 2, '<GDALMetadata><Item name="DESCRIPTION">A &amp; B</Item><Item name="STATISTICS_MINIMUM" sample="0">20</Item></GDALMetadata>\0'],
      );
      tags.sort((a, b) => a[0] - b[0]);
      levels.push({ tags, data });
    }
  }
  let cursor = 8;
  for (const level of levels) { level.offset = cursor; cursor += 2 + level.tags.length * 12 + 4; }
  const chunks = [];
  const allocate = (bytes, alignment = 8) => {
    cursor = Math.ceil(cursor / alignment) * alignment;
    const offset = cursor;
    chunks.push({ offset, bytes });
    cursor += bytes.length;
    return offset;
  };
  for (const level of levels) {
    level.entries = level.tags.map(([tag, type, values]) => {
      const count = values.length;
      const bytes = type === 2 ? Buffer.from(values) : Buffer.alloc(count * ({3:2, 4:4, 12:8}[type]));
      if (type !== 2) values.forEach((value, index) => {
        if (type === 3) bytes.writeUInt16LE(value, index * 2);
        if (type === 4) bytes.writeUInt32LE(value, index * 4);
        if (type === 12) bytes.writeDoubleLE(value, index * 8);
      });
      return { tag, type, count, bytes, offset: bytes.length > 4 ? allocate(bytes) : null };
    });
  }
  // Keep metadata before imagery, as in a COG. Larger alignment separates
  // compressed blocks from header reads for concurrency tests.
  for (const level of levels) {
    const offsets = level.entries.find(entry => entry.tag === 324).bytes;
    level.data.forEach((bytes, index) => offsets.writeUInt32LE(allocate(bytes, blockAlignment), index * 4));
  }
  const output = Buffer.alloc(cursor);
  output.write('II'); output.writeUInt16LE(42, 2); output.writeUInt32LE(8, 4);
  for (const [index, level] of levels.entries()) {
    output.writeUInt16LE(level.entries.length, level.offset);
    level.entries.forEach((entry, i) => {
      const offset = level.offset + 2 + i * 12;
      output.writeUInt16LE(entry.tag, offset); output.writeUInt16LE(entry.type, offset + 2);
      output.writeUInt32LE(entry.count, offset + 4);
      if (entry.offset !== null) output.writeUInt32LE(entry.offset, offset + 8);
      else entry.bytes.copy(output, offset + 8);
    });
    output.writeUInt32LE(levels[index + 1]?.offset || 0, level.offset + 2 + level.entries.length * 12);
  }
  for (const { offset, bytes } of chunks) bytes.copy(output, offset);
  return output;
}
