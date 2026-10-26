import { Tiler } from '@basemaps/tiler';
import { GoogleTms } from '@basemaps/geo';
import { ForbiddenError, ResourceUnavailableError, UnknownError } from './errors.js';
import { TileMakerPhoton } from './tile_maker_photon.js';
import { COGReader } from './cog_reader.js';
import { COGRangeCache } from './cog_range_cache.js';

const URL_WHITELIST = [
  'https://github.com/ramSeraph/',
  'http://127.0.0.1:8080/',
];
const tiler = new Tiler(GoogleTms);

function webMercatorToLatLng(x, y) {
  return {
    lng: (x / 6378137) * (180 / Math.PI),
    lat: (Math.atan(Math.exp(y / 6378137)) * 2 - Math.PI / 2) * (180 / Math.PI),
  };
}

class COGHandler {
  constructor(logger) {
    this.logger = logger;
    this.rangeCache = new COGRangeCache();
  }

  async getTiff(url) {
    if (!URL_WHITELIST.some(prefix => url.startsWith(prefix))) {
      throw new ForbiddenError('URL not in whitelist');
    }
    try {
      return await COGReader.open(url, this.rangeCache);
    } catch (err) {
      if (err.statusCode) throw err;
      if (/fetch|404|Not Found|ENOTFOUND/.test(err.message) || err.code === 'ENOTFOUND' || err.status === 404) {
        this.logger.error({ err }, `COG not found at URL: ${url}`);
        throw new ResourceUnavailableError(`COG not found at URL: ${url}`);
      }
      this.logger.error({ err }, `Failed to load COG from ${url}`);
      throw new UnknownError(`Failed to load COG: ${err.message}`);
    }
  }

  async getTile(url, z, x, y, format = 'png') {
    try {
      const reader = await this.getTiff(url);
      const layers = tiler.tile([reader], x, y, z);
      if (!layers?.length) return null;
      const outputFormat = format === 'webp' ? 'webp' : 'png';
      const result = await new TileMakerPhoton(256).compose({
        layers,
        format: outputFormat,
        background: { r: 0, g: 0, b: 0, alpha: 0 },
      });
      return { tile: result.buffer, mimeType: `image/${outputFormat}` };
    } catch (err) {
      if (err.statusCode) throw err;
      this.logger.error({ err }, 'Error getting COG tile');
      throw new UnknownError(`Error processing tile: ${err.message}`);
    }
  }

  async getInfo(url) {
    try {
      const { geotiff } = await this.getTiff(url);
      const image = geotiff.image;
      const bbox = geotiff.bbox;
      const sw = webMercatorToLatLng(bbox[0], bbox[1]);
      const ne = webMercatorToLatLng(bbox[2], bbox[3]);
      return {
        bbox: [sw.lng, sw.lat, ne.lng, ne.lat],
        center: [(sw.lng + ne.lng) / 2, (sw.lat + ne.lat) / 2],
        bounds: { west: sw.lng, south: sw.lat, east: ne.lng, north: ne.lat },
        size: { width: geotiff.width, height: geotiff.height },
        tileSize: { width: geotiff.tileWidth, height: geotiff.tileHeight },
        resolution: image.resolution,
        imageCount: geotiff.tiff.images.length,
        compression: image.compression,
        photometric: image.photometric,
      };
    } catch (err) {
      if (err.statusCode) throw err;
      this.logger.error({ err }, 'Error getting COG info');
      throw new UnknownError(`Error getting COG info: ${err.message}`);
    }
  }
}

export default COGHandler;
