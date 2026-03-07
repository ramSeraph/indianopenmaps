// Base class for partial download format handlers

export function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

export class FormatHandler {
  constructor({ tabId, conn, db, urls, bbox } = {}) {
    this.tabId = tabId;
    this.conn = conn;
    this.db = db;
    this.urls = urls || [];
    this.bbox = bbox;
    this.tempFiles = [];
    this._opfsPath = null;
    this._prepared = false;
    this._outputFileName = null;
  }

  get extension() { throw new Error('Not implemented'); }
  get needsDuckDBRegistration() { return true; }

  get parquetSource() {
    const urlList = this.urls.map(u => `'${u}'`).join(', ');
    return `read_parquet([${urlList}], union_by_name=true)`;
  }

  trackTempFile(opfsFileName) {
    this.tempFiles.push(opfsFileName);
  }

  async releaseTempFile(opfsFileName) {
    try {
      const root = await navigator.storage.getDirectory();
      await root.removeEntry(opfsFileName);
    } catch (e) { /* may already be cleaned up */ }
    this.tempFiles = this.tempFiles.filter(f => f !== opfsFileName);
  }

  async prepareOpfs() {
    this._opfsPath = `opfs://partial_${this.tabId}_${Date.now()}${this.extension}`;
    this._outputFileName = this._opfsPath.replace('opfs://', '');
    if (this.needsDuckDBRegistration) {
      await this.db.registerOPFSFileName(this._opfsPath);
      this._prepared = true;
      await sleep(5);
    }
  }

  async releaseOpfs() {
    if (this._prepared) {
      await this.db.dropFile(this._opfsPath);
      this._prepared = false;
    }
  }

  get outputFileName() { return this._outputFileName; }

  wrapBlobParts(file) { return [file]; }

  get bboxWkt() {
    return `POLYGON((${this.bbox.west} ${this.bbox.south}, ${this.bbox.east} ${this.bbox.south}, ${this.bbox.east} ${this.bbox.north}, ${this.bbox.west} ${this.bbox.north}, ${this.bbox.west} ${this.bbox.south}))`;
  }

  get bboxFilter() {
    return `ST_Intersects(geometry, ST_GeomFromText('${this.bboxWkt}'))`;
  }

  async write(_callbacks) {
    throw new Error('Not implemented');
  }

  async cleanup() {
    try {
      await this.releaseOpfs();
    } catch (e) { /* may already be released */ }
    await this.cleanupTempFiles();
    this._opfsPath = null;
  }

  async cleanupTempFiles() {
    const root = await navigator.storage.getDirectory();
    for (const fileName of this.tempFiles) {
      try {
        await root.removeEntry(fileName);
      } catch (e) { /* may already be cleaned up */ }
    }
    this.tempFiles = [];
  }
}
