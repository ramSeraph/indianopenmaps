export function formatSize(bytes) {
  if (bytes === 0) return '0 B';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + ' ' + sizes[i];
}

export function proxyUrl(url, { absolute = false } = {}) {
  const path = `/proxy?url=${encodeURIComponent(url)}`;
  return absolute ? `${window.location.origin}${path}` : path;
}

// OPFS file/dir name prefixes — each followed by TAB_ID.
// Longest prefixes first so extractTabId matches greedily.
const OPFS_PREFIXES = [];
function opfsPrefix(value) {
  OPFS_PREFIXES.push(value);
  return value;
}

export const OPFS_PREFIX_GPKG_TMP = opfsPrefix('dl_gpkg_tmp_');
export const OPFS_PREFIX_GPKG = opfsPrefix('dl_gpkg_');
export const OPFS_PREFIX_OUTPUT = opfsPrefix('dl_output_');
export const OPFS_PREFIX_TMP = opfsPrefix('dl_tmp_');
export const OPFS_PREFIX_TMPDIR = opfsPrefix('tmpdir_');

export function getOpfsPrefixes() {
  return OPFS_PREFIXES;
}

/**
 * Maps 0–100 progress to a sub-range of a parent progress handler.
 * Supports nesting: a ScopedProgress can wrap another ScopedProgress.
 */
export class ScopedProgress {
  constructor(onProgress, start, end) {
    this._onProgress = onProgress;
    this._start = start;
    this._end = end;
    this.callback = this.report.bind(this);
  }

  report(pct) {
    const clamped = Math.max(0, Math.min(100, pct));
    const mapped = this._start + (clamped / 100) * (this._end - this._start);
    this._onProgress?.(Math.round(mapped));
  }
}

export async function getStorageEstimate() {
  const { usage, quota } = await navigator.storage.estimate();
  return { usage, quota };
}
