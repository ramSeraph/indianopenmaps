const { Tiff } = require('@cogeotiff/core');
const { SourceUrl } = require('@cogeotiff/source-url');
const { Tiler } = require('@basemaps/tiler');
const { TileMakerSharp } = require('@basemaps/tiler-sharp');
const { GoogleTms } = require('@basemaps/geo');
const sharp = require('sharp');
const { inflateSync } = require('zlib');
const { ForbiddenError, ResourceUnavailableError, UnknownError } = require('./errors');

const URL_WHITELIST = [
  'https://github.com/ramSeraph/'
];

const tiler = new Tiler(GoogleTms);

class COGHandler {
  constructor(logger) {
    this.logger = logger;
    this.tiffCache = new Map();
  }

  isUrlAllowed(url) {
    return URL_WHITELIST.some(prefix => url.startsWith(prefix));
  }

  calculateGeotransform(image) {
    // ModelPixelScale tag (33550): [ScaleX, ScaleY, ScaleZ]
    const pixelScale = image.value(33550);
    // ModelTiepoint tag (33922): [I, J, K, X, Y, Z]
    const tiepoint = image.value(33922);
    
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
    if (!this.isUrlAllowed(url)) {
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
      
      if (this.tiffCache.size > 50) {
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

  // Convert Web Mercator (EPSG:3857) to lat/lng (EPSG:4326)
  webMercatorToLatLng(x, y) {
    const lng = (x / 6378137) * (180 / Math.PI);
    const lat = (Math.atan(Math.exp(y / 6378137)) * 2 - Math.PI / 2) * (180 / Math.PI);
    return { lat, lng };
  }

  async extractMaskTile(maskImage, comp) {
    // Get tile coordinates from composition
    const { source } = comp;
    const tileX = Math.floor(source.x / maskImage.tileSize.width);
    const tileY = Math.floor(source.y / maskImage.tileSize.height);
    
    try {
      const maskTile = await maskImage.getTile(tileX, tileY);
      if (!maskTile || !maskTile.bytes) {
        return null;
      }
      
      // Decode tile data
      let maskData = Buffer.from(maskTile.bytes);
      if (maskTile.mimeType === 'application/deflate') {
        maskData = inflateSync(maskData);
      }
      
      // Get tile bounds
      const tileBounds = maskImage.getTileBounds(tileX, tileY);
      const { width, height } = tileBounds;
      
      // Check bits per sample
      const bitsPerSample = maskImage.value(258)?.[0] || 1;
      let alphaBuffer;
      
      if (bitsPerSample === 1) {
        // Unpack 1-bit mask
        alphaBuffer = Buffer.alloc(width * height);
        for (let i = 0; i < width * height; i++) {
          const byteIdx = Math.floor(i / 8);
          const bitIdx = 7 - (i % 8);
          const bit = (maskData[byteIdx] >> bitIdx) & 1;
          alphaBuffer[i] = bit * 255;
        }
      } else {
        alphaBuffer = maskData;
      }
      
      return { buffer: alphaBuffer, width, height };
    } catch (err) {
      if (err.message && err.message.includes('outside of range')) {
        return null;
      }
      throw err;
    }
  }

  async applyMask(tileBuffer, maskBuffer, width, height) {
    // Convert tile to raw RGBA
    const tileImage = sharp(tileBuffer);
    const tileRaw = await tileImage.ensureAlpha().raw().toBuffer();
    
    // Ensure we have an alpha channel (4 channels)
    if (tileRaw.length !== width * height * 4) {
      throw new Error('Expected RGBA data');
    }
    
    // Apply mask to alpha channel
    for (let i = 0; i < width * height; i++) {
      tileRaw[i * 4 + 3] = maskBuffer[i];
    }
    
    return tileRaw;
  }

  async getTile(url, z, x, y, format = 'png') {
    try {
      const tiff = await this.getTiff(url);
      
      // Build map of RGB images to their corresponding masks
      const imageMaskMap = new Map();
      const maskImages = [];
      
      for (const img of tiff.images) {
        const subfileType = img.value(254);
        if (subfileType === 4 || subfileType === 5) {
          maskImages.push(img);
        }
      }
      
      // Match masks to RGB images by dimensions
      for (const maskImg of maskImages) {
        for (const rgbImg of tiff.images) {
          const subfileType = rgbImg.value(254);
          if (subfileType !== 4 && subfileType !== 5 &&
              rgbImg.size.width === maskImg.size.width &&
              rgbImg.size.height === maskImg.size.height) {
            imageMaskMap.set(rgbImg, maskImg);
            break;
          }
        }
      }
      
      // Create a filtered view of the tiff with only valid RGB images
      const validImages = tiff.images.filter(img => {
        try {
          // Filter out masks and images without resolution
          const subfileType = img.value(254);
          if (subfileType === 4 || subfileType === 5) return false;
          return img.resolution;
        } catch (e) {
          return false;
        }
      });
      
      // Create a wrapper tiff object with filtered images
      const wrappedTiff = Object.create(tiff);
      wrappedTiff.images = validImages.map(img => {
        // Wrap getTile to catch out-of-bounds errors
        const wrappedImg = Object.create(img);
        const originalGetTile = img.getTile.bind(img);
        wrappedImg.getTile = function(x, y) {
          try {
            return originalGetTile(x, y);
          } catch (err) {
            if (err.message && err.message.includes('outside of range')) {
              return null;
            }
            throw err;
          }
        };
        return wrappedImg;
      });
      
      const compositions = tiler.tile([wrappedTiff], x, y, z);
      
      if (!compositions || compositions.length === 0) {
        return null;
      }
      
      const tileMaker = new TileMakerSharp(256);
      const outputFormat = (format === 'webp') ? 'webp' : 'png';
      
      const result = await tileMaker.compose({
        layers: compositions,
        format: outputFormat,
        background: { r: 0, g: 0, b: 0, alpha: 255 },
        resizeKernel: { in: 'nearest', out: 'lanczos3' }
      });
      
      // Try to apply mask if available
      try {
        if (compositions.length > 0 && compositions[0].type === 'tiff') {
          // Get the RGB image that was used
          const comp = compositions[0];
          const usedRgbImage = comp.asset.images.find(img => img.id === comp.source.imageId);
          
          if (usedRgbImage && imageMaskMap.has(usedRgbImage)) {
            const maskImage = imageMaskMap.get(usedRgbImage);
            
            // Manually extract and process mask tile
            const maskData = await this.extractMaskTile(maskImage, comp);
            
            if (maskData) {
              const { buffer: maskBuffer, width, height } = maskData;
              
              // Resize mask to 256x256 if needed
              let resizedMask;
              if (width !== 256 || height !== 256) {
                resizedMask = await sharp(maskBuffer, {
                  raw: { width, height, channels: 1 }
                })
                .resize(256, 256, { kernel: 'nearest' })
                .raw()
                .toBuffer();
              } else {
                resizedMask = maskBuffer;
              }
              
              const maskedRgba = await this.applyMask(result.buffer, resizedMask, 256, 256);
              
              const finalBuffer = await sharp(maskedRgba, {
                raw: { width: 256, height: 256, channels: 4 }
              })
              .toFormat(outputFormat === 'webp' ? 'webp' : 'png')
              .toBuffer();
              
              return { tile: finalBuffer, mimeType: `image/${outputFormat}` };
            }
          }
        }
      } catch (maskErr) {
        // Log but don't fail - return tile without mask
        this.logger.warn({ err: maskErr }, 'Failed to apply mask');
      }
      
      return { tile: result.buffer, mimeType: `image/${outputFormat}` };
    } catch (err) {
      // Re-throw known errors
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
      
      const sw = this.webMercatorToLatLng(bbox[0], bbox[1]);
      const ne = this.webMercatorToLatLng(bbox[2], bbox[3]);
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
