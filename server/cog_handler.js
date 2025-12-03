const { Tiff, TiffTag, SubFileType, Compression } = require('@cogeotiff/core');
const { SourceUrl } = require('@cogeotiff/source-url');
const { Tiler } = require('@basemaps/tiler');
const { TileMakerSharp } = require('@basemaps/tiler-sharp');
const { GoogleTms } = require('@basemaps/geo');
const sharp = require('sharp');
const { inflateSync } = require('zlib');
const { ForbiddenError, ResourceUnavailableError, UnknownError } = require('./errors');

/**
 * IMPORTANT: This COG handler with mask support assumes that ALL source TIFFs
 * either have internal masks or none of them do. Mixing TIFFs with and without
 * masks will produce incorrect results - tiles from mask-less TIFFs will appear
 * fully transparent when composited with tiles from masked TIFFs.
 * 
 * If you need to support mixed sources, you'll need to modify the alpha channel
 * handling to default to fully opaque (255) for TIFFs without masks.
 */

const URL_WHITELIST = [
  'https://github.com/ramSeraph/',
  'http://127.0.0.1:8080/',
];

const TIFF_CACHE_SIZE = 100;

// Utility function for mask image detection
function isMaskImage(img) {
  const subfileType = img.value(TiffTag.SubFileType);
  return !!(subfileType & SubFileType.Mask);
}

// Convert Web Mercator (EPSG:3857) to lat/lng (EPSG:4326)
function webMercatorToLatLng(x, y) {
  const lng = (x / 6378137) * (180 / Math.PI);
  const lat = (Math.atan(Math.exp(y / 6378137)) * 2 - Math.PI / 2) * (180 / Math.PI);
  return { lat, lng };
}

function isUrlAllowed(url) {
  return URL_WHITELIST.some(prefix => url.startsWith(prefix));
}

class FilteredTiff extends Tiff {
  constructor(tiff, filterFn) {
    // Call parent constructor with the same source
    super(tiff.source);
    this._originalTiff = tiff;
    this.filterFn = filterFn;
    this._filteredImages = null;
    
    // Copy over initialized state from original tiff
    this.isLittleEndian = tiff.isLittleEndian;
    this.version = tiff.version;
    this.options = tiff.options;
    this.ifdConfig = tiff.ifdConfig;
    this.isInitialized = tiff.isInitialized;
    
    // Delete parent's images property so our getter is used
    delete this.images;
  }

  _applyImageModifications() {
    // Get the first non-mask image for geo transformation reference
    const refImage = this._originalTiff.images.find(img => !isMaskImage(img));
    
    for (const img of this._filteredImages) {
      if (img._modified) continue;
      img._modified = true;

      const originalGetTile = img.getTile;
      
      // Fix isSubImage to use bitwise check for proper overview detection
      Object.defineProperty(img, 'isSubImage', {
        get: function() {
          const subfileType = this.value(TiffTag.SubFileType);
          return !!(subfileType & SubFileType.ReducedImage);
        },
        configurable: true
      });
      
      // For mask images, use origin and resolution from corresponding RGB image
      if (isMaskImage(img) && refImage) {
        Object.defineProperty(img, 'origin', {
          get: function() {
            return refImage.origin;
          },
          configurable: true
        });
        
        Object.defineProperty(img, 'resolution', {
          get: function() {
            const [resX, resY, resZ] = refImage.resolution;
            const refSize = refImage.size;
            const imgSize = this.size;
            return [
              (resX * refSize.width) / imgSize.width,
              (resY * refSize.height) / imgSize.height,
              resZ
            ];
          },
          configurable: true
        });
      }
      
      img.getTile = async function(tx, ty) {
        const tile = await originalGetTile.call(this, tx, ty);
        
        if (!isMaskImage(this)) {
          return tile;
        }

        // Validate single channel for mask
        const bitsPerSampleArr = this.value(TiffTag.BitsPerSample);
        if (bitsPerSampleArr.length !== 1) {
          throw new UnknownError('Mask image must have exactly 1 channel');
        }

        // Decompress if needed
        let maskData;
        if (tile.compression === Compression.Deflate || tile.compression === Compression.DeflateOther) {
          maskData = inflateSync(Buffer.from(tile.bytes));
        } else if (tile.compression === Compression.None) {
          maskData = Buffer.from(tile.bytes);
        } else {
          throw new UnknownError(`Unsupported mask compression: ${tile.compression}`);
        }

        const bitsPerSample = bitsPerSampleArr[0];
        const tileWidth = this.tileSize.width;
        const tileHeight = this.tileSize.height;

        // Expand to 8-bit alpha
        let alphaChannel;
        if (bitsPerSample === 1) {
          // 1-bit packed: manual expansion (sharp can't read 1-bit packed)
          alphaChannel = Buffer.alloc(tileWidth * tileHeight);
          for (let i = 0; i < tileWidth * tileHeight; i++) {
            const byteIdx = Math.floor(i / 8);
            const bitIdx = 7 - (i % 8);
            const bit = (maskData[byteIdx] >> bitIdx) & 1;
            alphaChannel[i] = bit * 255;
          }
        } else if (bitsPerSample === 8) {
          alphaChannel = maskData;
        } else {
          throw new UnknownError(`Unsupported mask bitsPerSample: ${bitsPerSample}`);
        }

        // Create RGBA PNG with black RGB + alpha (so TileMakerSharp can decode it)
        const pngBuffer = await sharp(Buffer.alloc(tileWidth * tileHeight * 3, 0), {
          raw: { width: tileWidth, height: tileHeight, channels: 3 }
        })
          .joinChannel(alphaChannel, { raw: { width: tileWidth, height: tileHeight, channels: 1 } })
          .png()
          .toBuffer();

        return {
          mimeType: 'image/png',
          bytes: pngBuffer,
          compression: Compression.None
        };
      };
    }
  }

  get images() {
    if (this._filteredImages === null) {
      this._filteredImages = this._originalTiff.images.filter(this.filterFn);
      this._applyImageModifications();
    }
    return this._filteredImages;
  }
}

class MaskAwareTiler extends Tiler {
  tile(sources, x, y, z) {
    const result = {
      rgbComps: [],
      maskComps: []
    };
    
    for (const source of sources) {
      // Always create both RGB and mask filtered views
      const rgbTiff = new FilteredTiff(source, img => !isMaskImage(img));
      const maskTiff = new FilteredTiff(source, img => isMaskImage(img));
      
      // Only call super.tile if there are images to process
      if (rgbTiff.images.length > 0) {
        const rgbComps = super.tile([rgbTiff], x, y, z);
        if (rgbComps) result.rgbComps.push(...rgbComps);
      }
      
      if (maskTiff.images.length > 0) {
        const maskComps = super.tile([maskTiff], x, y, z);
        if (maskComps) result.maskComps.push(...maskComps);
      }
    }
    
    return result;
  }
}

const tiler = new MaskAwareTiler(GoogleTms);

class COGHandler {
  constructor(logger) {
    this.logger = logger;
    this.tiffCache = new Map();
  }

  calculateGeotransform(image) {
    // ModelPixelScale tag: [ScaleX, ScaleY, ScaleZ]
    const pixelScale = image.value(TiffTag.ModelPixelScale);
    // ModelTiepoint tag: [I, J, K, X, Y, Z]
    const tiepoint = image.value(TiffTag.ModelTiePoint);
    
    if (!pixelScale || !tiepoint) {
      return null;
    }
    
    // Extract values
    const scaleX = pixelScale[0];
    const scaleY = -Math.abs(pixelScale[1]); // Usually negative for north-up images
    const originX = tiepoint[3]; // X coordinate at I,J = 0,0
    const originY = tiepoint[4]; // Y coordinate at I,J = 0,0
    
    // Geotransform: [originX, pixelWidth, 0, originY, 0, pixelHeight]
    return [originX, scaleX, 0, originY, 0, scaleY];
  }

  async getTiff(url) {
    if (!isUrlAllowed(url)) {
      throw new ForbiddenError('URL not in whitelist');
    }

    if (this.tiffCache.has(url)) {
      return this.tiffCache.get(url);
    }

    try {
      const source = new SourceUrl(url);
      // @basemaps/tiler expects source.url to be a URL object
      source.url = new URL(url);
      
      source.fetch = async function(offset, length) {
        await this.loadBytes(offset, length);
        return this.bytes(offset, length).buffer;
      };
      
      const tiff = new Tiff(source);
      await tiff.init();
      
      this.tiffCache.set(url, tiff);
      
      if (this.tiffCache.size > TIFF_CACHE_SIZE) {
        const firstKey = this.tiffCache.keys().next().value;
        this.tiffCache.delete(firstKey);
      }

      return tiff;
    } catch (err) {
      // Convert network/fetch errors to ResourceUnavailableError
      if (err.message.includes('fetch') || 
          err.message.includes('404') ||
          err.message.includes('Not Found') ||
          err.message.includes('ENOTFOUND') ||
          err.code === 'ENOTFOUND' ||
          err.statusCode === 404) {
        this.logger.error({ err }, `COG not found at URL: ${url}`);
        throw new ResourceUnavailableError(`COG not found at URL: ${url}`);
      }
      this.logger.error({ err }, `Failed to load COG from ${url}`);
      throw new UnknownError(`Failed to load COG: ${err.message}`);
    }
  }

  async getTile(url, z, x, y, format = 'png') {
    try {
      const tiff = await this.getTiff(url);
      
      const { rgbComps, maskComps } = tiler.tile([tiff], x, y, z);
      
      if (!rgbComps || rgbComps.length === 0) {
        return null;
      }

      const outputFormat = (format === 'webp') ? 'webp' : 'png';
      const tileMaker = new TileMakerSharp(256);

      // If no mask layers, just render RGB
      if (!maskComps || maskComps.length === 0) {
        const rgbResult = await tileMaker.compose({
          layers: rgbComps,
          format: outputFormat,
          background: { r: 0, g: 0, b: 0, alpha: 0 },
          resizeKernel: { in: 'lanczos3', out: 'lanczos3' }
        });
        return { tile: rgbResult.buffer, mimeType: `image/${outputFormat}` };
      }

      // Compose RGB and mask tiles in parallel
      const [rgbResult, maskResult] = await Promise.all([
        tileMaker.compose({
          layers: rgbComps,
          format: 'png',
          background: { r: 0, g: 0, b: 0, alpha: 0 },
          resizeKernel: { in: 'lanczos3', out: 'lanczos3' }
        }),
        tileMaker.compose({
          layers: maskComps,
          format: 'png',
          background: { r: 0, g: 0, b: 0, alpha: 0 },
          resizeKernel: { in: 'lanczos3', out: 'lanczos3' }
        })
      ]);

      // Extract RGB from rgbResult and alpha from maskResult, then combine
      const [rgbRaw, maskRaw] = await Promise.all([
        sharp(rgbResult.buffer).removeAlpha().raw().toBuffer({ resolveWithObject: true }),
        sharp(maskResult.buffer).extractChannel('alpha').raw().toBuffer()
      ]);

      // Join RGB with mask alpha
      const finalSharp = sharp(rgbRaw.data, {
        raw: { width: rgbRaw.info.width, height: rgbRaw.info.height, channels: 3 }
      }).joinChannel(maskRaw, {
        raw: { width: 256, height: 256, channels: 1 }
      });

      const result = outputFormat === 'webp' 
        ? await finalSharp.webp().toBuffer()
        : await finalSharp.png().toBuffer();
      
      return { tile: result, mimeType: `image/${outputFormat}` };
    } catch (err) {
      if (err.statusCode) {
        throw err;
      }
      this.logger.error({ err }, 'Error getting COG tile');
      throw new UnknownError(`Error processing tile: ${err.message}`);
    }
  }

  async getInfo(url) {
    try {
      const tiff = await this.getTiff(url);
      const image = tiff.images[0];
      
      // Get geotransform
      const geotransform = this.calculateGeotransform(image);
      if (!geotransform) {
        throw new UnknownError('No geotransform available in COG');
      }
      
      // Calculate bbox
      let bbox;
      try {
        bbox = image.bbox;
      } catch (err) {
        // Compute bbox manually from geotransform
        const [originX, pixelWidth, _, originY, __, pixelHeight] = geotransform;
        const width = image.size.width;
        const height = image.size.height;
        
        const minX = originX;
        const maxY = originY;
        const maxX = originX + (width * pixelWidth);
        const minY = originY + (height * pixelHeight);
        
        bbox = [minX, minY, maxX, maxY];
      }
      
      const sw = webMercatorToLatLng(bbox[0], bbox[1]);
      const ne = webMercatorToLatLng(bbox[2], bbox[3]);
      let bboxLatLng = [sw.lng, sw.lat, ne.lng, ne.lat];
      
      // Get resolution
      let resolution;
      try {
        resolution = image.resolution;
      } catch (err) {
        resolution = [Math.abs(geotransform[1]), Math.abs(geotransform[5])];
      }
      
      // Calculate center
      const center = [
        (bboxLatLng[0] + bboxLatLng[2]) / 2,
        (bboxLatLng[1] + bboxLatLng[3]) / 2
      ];
      
      return {
        bbox: bboxLatLng,
        center: center,
        bounds: {
          west: bboxLatLng[0],
          south: bboxLatLng[1],
          east: bboxLatLng[2],
          north: bboxLatLng[3]
        },
        size: {
          width: image.size.width,
          height: image.size.height
        },
        tileSize: {
          width: image.tileSize.width,
          height: image.tileSize.height
        },
        resolution: resolution,
        imageCount: tiff.images.length,
        compression: image.compression,
        photometric: image.photometric
      };
    } catch (err) {
      if (err.statusCode) {
        throw err;
      }
      this.logger.error({ err }, 'Error getting COG info');
      throw new UnknownError(`Error getting COG info: ${err.message}`);
    }
  }
}

module.exports = COGHandler;
