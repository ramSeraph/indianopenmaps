import { PhotonImage, resize, crop, SamplingFilter } from '@cf-wasm/photon';

/**
 * Tile compositor using @cf-wasm/photon (WASM) instead of Sharp.
 * Compatible with Cloudflare Workers.
 * 
 * Note: Photon produces lossless WebP (VP8L) while Sharp produces lossy (VP8).
 * Lossless WebP is larger but preserves quality perfectly.
 */
export class TileMakerPhoton {
  constructor(width, height = width) {
    this.width = width;
    this.height = height;
  }

  /**
   * Compose multiple tile layers into a single output image.
   * @param {Object} ctx - Composition context
   * @param {Array} ctx.layers - Array of composition layers from @basemaps/tiler
   * @param {string} ctx.format - Output format ('png' or 'webp')
   * @param {Object} ctx.background - Background color { r, g, b, alpha }
   * @param {Object} ctx.resizeKernel - Resize kernel options (ignored, uses Lanczos3)
   * @returns {Promise<{buffer: Uint8Array, layers: number}>}
   */
  async compose(ctx) {
    const overlays = [];

    for (const comp of ctx.layers) {
      const overlay = await this.composeTile(comp);
      if (overlay) {
        overlays.push(overlay);
      }
    }

    if (overlays.length === 0) {
      // Return empty transparent image
      const buffer = this.createEmptyImage(ctx.format, ctx.background);
      return { buffer, layers: 0 };
    }

    // Create base canvas pixels with background
    const canvasPixels = this.createCanvasPixels(ctx.background);

    // Draw each overlay onto canvas pixels
    for (const overlay of overlays) {
      this.drawOverlay(canvasPixels, overlay);
    }

    // Convert to PhotonImage and export
    const canvas = new PhotonImage(canvasPixels, this.width, this.height);
    const buffer = ctx.format === 'webp'
      ? canvas.get_bytes_webp()
      : canvas.get_bytes();

    canvas.free();

    return { buffer, layers: overlays.length };
  }

  /**
   * Process a single composition layer from tiff source
   */
  async composeTile(comp) {
    if (comp.type !== 'tiff') {
      return null;
    }

    const tile = await comp.asset.images[comp.source.imageId].getTile(comp.source.x, comp.source.y);
    if (tile == null) {
      return null;
    }

    // Decode the tile bytes into a PhotonImage
    let img = PhotonImage.new_from_byteslice(new Uint8Array(tile.bytes));

    const { extract, resize: resizeOp, crop: cropOp } = comp;

    // Extract: limit to first extract.width x extract.height pixels
    if (extract) {
      const cropped = crop(img, 0, 0, extract.width, extract.height);
      img.free();
      img = cropped;
    }

    // Resize if needed
    if (resizeOp) {
      const resized = resize(img, resizeOp.width, resizeOp.height, SamplingFilter.Lanczos3);
      img.free();
      img = resized;
    }

    // Crop after resize
    if (cropOp) {
      const cropped = crop(img, cropOp.x, cropOp.y, cropOp.x + cropOp.width, cropOp.y + cropOp.height);
      img.free();
      img = cropped;
    }

    const width = img.get_width();
    const height = img.get_height();
    const pixels = img.get_raw_pixels();
    img.free();

    return {
      pixels,
      width,
      height,
      x: comp.x,
      y: comp.y,
    };
  }

  /**
   * Create canvas pixels filled with background color
   */
  createCanvasPixels(background) {
    const size = this.width * this.height * 4;
    const pixels = new Uint8Array(size);

    const r = background.r || 0;
    const g = background.g || 0;
    const b = background.b || 0;
    const a = Math.round((background.alpha ?? 1) * 255);

    for (let i = 0; i < size; i += 4) {
      pixels[i] = r;
      pixels[i + 1] = g;
      pixels[i + 2] = b;
      pixels[i + 3] = a;
    }

    return pixels;
  }

  /**
   * Draw overlay pixels onto canvas pixels at position (x, y)
   * Simple copy - no alpha blending needed for adjacent strips
   */
  drawOverlay(canvasPixels, overlay) {
    const { pixels, width, height, x, y } = overlay;

    // Determine the number of channels (3 for RGB, 4 for RGBA)
    const totalPixels = width * height;
    const channels = pixels.length / totalPixels;

    for (let row = 0; row < height; row++) {
      const canvasY = y + row;
      if (canvasY < 0 || canvasY >= this.height) continue;

      for (let col = 0; col < width; col++) {
        const canvasX = x + col;
        if (canvasX < 0 || canvasX >= this.width) continue;

        const srcIdx = (row * width + col) * channels;
        const dstIdx = (canvasY * this.width + canvasX) * 4;

        if (channels === 4) {
          // RGBA - copy directly (or blend if needed)
          const srcAlpha = pixels[srcIdx + 3];
          if (srcAlpha > 0) {
            canvasPixels[dstIdx] = pixels[srcIdx];
            canvasPixels[dstIdx + 1] = pixels[srcIdx + 1];
            canvasPixels[dstIdx + 2] = pixels[srcIdx + 2];
            canvasPixels[dstIdx + 3] = srcAlpha;
          }
        } else if (channels === 3) {
          // RGB - copy with full alpha
          canvasPixels[dstIdx] = pixels[srcIdx];
          canvasPixels[dstIdx + 1] = pixels[srcIdx + 1];
          canvasPixels[dstIdx + 2] = pixels[srcIdx + 2];
          canvasPixels[dstIdx + 3] = 255;
        }
      }
    }
  }

  /**
   * Create an empty image with background color
   */
  createEmptyImage(format, background) {
    const pixels = this.createCanvasPixels(background);
    const canvas = new PhotonImage(pixels, this.width, this.height);
    const buffer = format === 'webp'
      ? canvas.get_bytes_webp()
      : canvas.get_bytes();
    canvas.free();
    return buffer;
  }
}

/**
 * Helper to extract alpha channel from RGBA pixels
 */
export function extractAlphaChannel(pixels, width, height) {
  const alpha = new Uint8Array(width * height);
  for (let i = 0; i < alpha.length; i++) {
    alpha[i] = pixels[i * 4 + 3];
  }
  return alpha;
}

/**
 * Helper to join RGB pixels with separate alpha channel
 */
export function joinRgbWithAlpha(rgbPixels, alphaPixels, width, height) {
  const rgba = new Uint8Array(width * height * 4);
  const pixelCount = width * height;

  // rgbPixels might be RGB (3 channels) or RGBA (4 channels)
  const rgbChannels = rgbPixels.length / pixelCount;

  for (let i = 0; i < pixelCount; i++) {
    const srcIdx = i * rgbChannels;
    const dstIdx = i * 4;
    rgba[dstIdx] = rgbPixels[srcIdx];
    rgba[dstIdx + 1] = rgbPixels[srcIdx + 1];
    rgba[dstIdx + 2] = rgbPixels[srcIdx + 2];
    rgba[dstIdx + 3] = alphaPixels[i];
  }

  return rgba;
}

/**
 * Convert raw RGBA pixels to PNG or WebP
 */
export function rawToImage(pixels, width, height, format) {
  const img = new PhotonImage(pixels, width, height);
  const buffer = format === 'webp'
    ? img.get_bytes_webp()
    : img.get_bytes();
  img.free();
  return buffer;
}
