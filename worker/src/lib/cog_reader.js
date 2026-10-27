import { Compression, Photometric, TiffTag } from '@cogeotiff/core';
import { GeoTIFF, DecoderPool, toPixelInterleaved } from '@developmentseed/geotiff';
import { DOMParser as XmlDOMParser } from '@xmldom/xmldom';
import { PhotonImage } from '@cf-wasm/photon';
import { SourceChunk } from '@chunkd/middleware';
import { SourceView } from '@chunkd/source';
import { SourceHttp } from '@chunkd/source-http';
import { BadRequestError } from './errors.js';
import { fetchWithRetry } from './request.js';

// Use SourceHttp's supported fetch hook for both HEAD and range requests.
SourceHttp.fetch = fetchWithRetry;

// geotiff 0.9 parses GDAL's XML with DOMParser and querySelectorAll('Item').
// Workers has neither DOM API; supply only the selector used by this pinned
// dependency, backed by a real XML parser (including entity decoding).
if (typeof globalThis.DOMParser === 'undefined') {
  globalThis.DOMParser = class {
    parseFromString(xml, type) {
      const document = new XmlDOMParser().parseFromString(xml, type);
      document.documentElement.querySelectorAll = selector => {
        if (selector !== 'Item') throw new Error(`Unsupported XML selector: ${selector}`);
        return document.documentElement.getElementsByTagName(selector);
      };
      return document;
    }
  };
}

// The upstream JPEG/WebP decoder uses createImageBitmap + OffscreenCanvas.
// Photon supplies the same decoded samples in workerd. Other codecs, predictors,
// and packed masks are decoded by Development Seed, without Web Workers.
class WorkerDecoderPool extends DecoderPool {
  constructor() { super({ size: 0 }); }

  async decode(bytes, compression, metadata) {
    if (![Compression.Jpeg, Compression.Jpeg6, Compression.Webp].includes(compression)) {
      return super.decode(bytes, compression, metadata);
    }
    const image = PhotonImage.new_from_byteslice(new Uint8Array(bytes));
    try {
      const rgba = image.get_raw_pixels();
      const count = metadata.samplesPerPixel;
      if (count === 4) return { layout: 'pixel-interleaved', data: rgba };
      if (![1, 2, 3].includes(count)) throw new Error(`Unsupported image band count: ${count}`);
      const data = new Uint8Array(image.get_width() * image.get_height() * count);
      for (let src = 0, dst = 0; src < rgba.length; src += 4) {
        data[dst++] = rgba[src];
        if (count === 2) data[dst++] = rgba[src + 3];
        if (count === 3) {
          data[dst++] = rgba[src + 1];
          data[dst++] = rgba[src + 2];
        }
      }
      return { layout: 'pixel-interleaved', data };
    } finally {
      image.free();
    }
  }
}
const decoderPool = new WorkerDecoderPool();

/** Adapt GeoTIFF's data-only overview levels to the existing XYZ compositor. */
export class COGReader {
  constructor(geotiff, url) {
    this.geotiff = geotiff;
    this.source = { url: new URL(url) };
    const [a, b, , d, e] = geotiff.transform;
    if (geotiff.crs !== 3857 || b !== 0 || d !== 0 || a <= 0 || e >= 0) {
      throw new BadRequestError('COG tile rendering requires a north-up EPSG:3857 image.');
    }
    this.images = [geotiff, ...geotiff.overviews].map((level, id) => ({
      id,
      tiff: this,
      bbox: geotiff.bbox,
      resolution: [level.transform[0], level.transform[4]],
      tileSize: { width: level.tileWidth, height: level.tileHeight },
      tileCount: level.tileCount,
      getTileBounds: (x, y) => level.image.getTileBounds(x, y),
      getTile: async (x, y) => {
        const { array } = await level.fetchTile(x, y, { pool: decoderPool });
        return { pixels: toRGBA(array, geotiff, level.image), width: array.width, height: array.height };
      },
    }));
  }

  static async open(url, cache) {
    // Each request owns its reader, pending reads, and concurrency queue. Only
    // completed header/image bytes are shared, so cancelled tiles cannot leave
    // another request waiting for I/O owned by a finished Worker invocation.
    const source = new SourceView(new SourceHttp(url, {}), [
      new SourceChunk({ size: 32 * 1024 }), cache.forRequest(),
    ]);
    const geotiff = await GeoTIFF.open({
      headerSource: source,
      // Decoders may transform raw, uncompressed samples in place. Keep the
      // cached block immutable even when a read exactly matches a whole block.
      dataSource: { fetch: async (...args) => (await source.fetch(...args)).slice(0) },
    });
    return new COGReader(geotiff, url);
  }

  getImageByResolution(resolution) {
    // Preserve the existing tiler's overview selection tolerance.
    return this.images.findLast(image => image.resolution[0] - resolution <= 0.01) || this.images[0];
  }
}

function toRGBA(raster, geotiff, image) {
  const { data, count, width, height, mask } = toPixelInterleaved(raster);
  if (!(data instanceof Uint8Array)) throw new BadRequestError('COG rendering requires 8-bit imagery.');
  const photometric = geotiff.image.value(TiffTag.Photometric);
  const compression = image.value(TiffTag.Compression);
  const decodedColor = [Compression.Jpeg, Compression.Jpeg6, Compression.Webp].includes(compression);
  const gray = [Photometric.MinIsBlack, Photometric.MinIsWhite].includes(photometric);
  if (!(gray && count <= 2) && !(count >= 3 && (photometric === Photometric.Rgb || decodedColor))) {
    throw new BadRequestError(`Unsupported COG color interpretation: ${photometric}`);
  }
  const alphaBand = geotiff.colorInterp.indexOf('alpha');
  const associatedAlpha = geotiff.image.value(TiffTag.ExtraSamples)?.[0] === 1 && !decodedColor;
  const rgba = new Uint8Array(width * height * 4);
  for (let pixel = 0; pixel < width * height; pixel++) {
    const src = pixel * count;
    const dst = pixel * 4;
    const alpha = alphaBand >= 0 ? data[src + alphaBand] : 255;
    const factor = associatedAlpha && alpha > 0 ? 255 / alpha : 1;
    for (let channel = 0; channel < 3; channel++) {
      const value = data[src + (gray ? 0 : channel)];
      rgba[dst + channel] = Math.min(255, Math.round(
        (gray && photometric === Photometric.MinIsWhite && !decodedColor ? 255 - value : value) * factor,
      ));
    }
    rgba[dst + 3] = Math.round(alpha * (mask ? mask[pixel] / 255 : 1));
  }
  return rgba;
}
