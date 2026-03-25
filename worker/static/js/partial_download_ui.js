// UI for partial (bbox-filtered) downloads.
import {
  GeoParquetExtractor,
  formatSize, getStorageEstimate,
} from 'geoparquet-extractor';
import { bootstrapDuckDB } from './utils.js';
import { IomMetadataProvider } from './iom_metadata_provider.js';

const FORMAT_OPTIONS = [
  { value: 'geopackage', label: 'GeoPackage (.gpkg)' },
  { value: 'geojson', label: 'GeoJSON' },
  { value: 'geojsonseq', label: 'GeoJSONSeq (.geojsonl)' },
  { value: 'geoparquet', label: 'GeoParquet (v1.1)' },
  { value: 'geoparquet2', label: 'GeoParquet (v2.0)' },
  { value: 'csv', label: 'CSV (WKT geometry)' },
  { value: 'shapefile', label: 'Shapefile (.shp)' },
  { value: 'kml', label: 'KML (.kml)' },
  { value: 'dxf', label: 'DXF (.dxf)' },
];

// Memory slider constants
const MEMORY_STEP = 128;
const MEMORY_MIN_MB = 512;

function getDeviceMaxMemoryMB() {
  const deviceMemGB = navigator.deviceMemory || 4;
  return Math.max(MEMORY_MIN_MB, Math.floor(deviceMemGB * 1024 * 0.75 / MEMORY_STEP) * MEMORY_STEP);
}

function getDefaultMemoryLimitMB() {
  const deviceMemGB = navigator.deviceMemory || 4;
  const halfMB = Math.floor(deviceMemGB * 1024 * 0.5 / MEMORY_STEP) * MEMORY_STEP;
  return Math.max(MEMORY_MIN_MB, Math.min(halfMB, getDeviceMaxMemoryMB()));
}

// Derive gpkg worker URL from the library's import map entry.
// esm.sh rewrites JS modules (breaking standalone workers), so for production
// we extract the version from the resolved URL and fetch the raw worker bundle
// from jsdelivr instead. For local dev (no @version in URL), we fall back to
// resolving relative to the library URL (e.g. localhost:8788/gpkg_worker.js).
const _libUrl = import.meta.resolve('geoparquet-extractor');
const _libVersion = _libUrl.match(/@([\d.]+)/)?.[1];
const GPKG_WORKER_URL = _libVersion
  ? `https://cdn.jsdelivr.net/npm/geoparquet-extractor@${_libVersion}/dist/gpkg_worker.js`
  : new URL('gpkg_worker.js', _libUrl).href;

const FORMAT_LABELS = Object.fromEntries(FORMAT_OPTIONS.map(f => [f.value, f.label]));

function formatMemory(mb) {
  return mb >= 1024 ? `${(mb / 1024).toFixed(1)} GB` : `${mb} MB`;
}

export class PartialDownloadUI {
  constructor({ map, routesHandler }) {
    this.map = map;
    this.routesHandler = routesHandler;
    this.selectedSource = null;
    this.extractor = null;
    this._duckdbPromise = null;

    // UI elements (created in createSection)
    this.section = null;
    this.formatSelect = null;
    this.startButton = null;
    this.cancelButton = null;
    this.progressContainer = null;
    this.progressBar = null;
    this.statusText = null;
    this.bboxContainer = null;
    this.memorySlider = null;
    this.memoryValue = null;
    this.downloadInfo = null;

    this.map.on('moveend', () => this._updateBboxDisplay());
  }

  createSection() {
    this.section = document.createElement('div');
    this.section.className = 'download-section partial-download-section';
    this.section.style.display = 'none';

    // Section heading
    const heading = document.createElement('div');
    heading.className = 'download-section-heading';

    const leftPart = document.createElement('div');
    leftPart.className = 'download-section-left';

    const label = document.createElement('span');
    label.textContent = 'Partial Download';

    leftPart.appendChild(label);

    const toggleIcon = document.createElement('span');
    toggleIcon.className = 'download-toggle-icon';
    toggleIcon.textContent = '▼';

    heading.appendChild(leftPart);
    heading.appendChild(toggleIcon);

    heading.addEventListener('click', () => {
      this.section.classList.toggle('collapsed');
    });

    this.section.appendChild(heading);

    // Content container
    const content = document.createElement('div');
    content.className = 'partial-download-content';

    this.bboxContainer = document.createElement('div');
    this.bboxContainer.className = 'bbox-display partial-bbox';
    content.appendChild(this.bboxContainer);

    content.appendChild(this._createFormatRow());
    content.appendChild(this._createMemoryRow());
    content.appendChild(this._createButtonsRow());
    content.appendChild(this._createProgressContainer());

    this.section.appendChild(content);

    // Populate bbox now that DOM is ready
    this._updateBboxDisplay();

    return this.section;
  }

  setSourcePath(path) {
    this.selectedSource = path;
  }

  setVisible(visible) {
    if (this.section) this.section.style.display = visible ? '' : 'none';
  }

  destroy() {
    this.extractor?.cancel();
  }

  async startDownload() {
    const sourcePath = this.selectedSource;
    const routes = this.routesHandler.getVectorSources();
    const routeInfo = routes[sourcePath];

    const bounds = this.map.getBounds();
    const bbox = [bounds.getWest(), bounds.getSouth(), bounds.getEast(), bounds.getNorth()];

    const format = this.formatSelect.value;

    const sourceName = routeInfo.name || sourcePath;
    const isPartitioned = routeInfo.partitioned_parquet === true;
    const memMB = parseInt(this.memorySlider.value);
    const memStr = formatMemory(memMB);

    this._setDownloadingState(true);

    const bboxDisplay = `${bbox[0].toFixed(4)}, ${bbox[1].toFixed(4)} → ${bbox[2].toFixed(4)}, ${bbox[3].toFixed(4)}`;
    this.downloadInfo.innerHTML =
      `<b>${sourceName}</b><br>` +
      `<span class="partial-download-info-detail">format: ${FORMAT_LABELS[format] || format}</span>` +
      `<span class="partial-download-info-detail">bbox: ${bboxDisplay}</span>` +
      `<span class="partial-download-info-detail">memory: ${memStr}</span>`;

    const callbacks = {
      onProgress: (pct) => this._updateProgress(pct),
      onStatus: (msg) => this._updateStatus(msg)
    };

    try {
      this._updateStatus('Preparing...');

      // Lazy-init DuckDB and create extractor on first download
      if (!this.extractor) {
        if (!this._duckdbPromise) this._duckdbPromise = bootstrapDuckDB();
        const duckdb = await this._duckdbPromise;
        this.extractor = new GeoParquetExtractor({
          duckdb,
          metadataProvider: new IomMetadataProvider(),
          gpkgWorkerUrl: GPKG_WORKER_URL,
        });
        GeoParquetExtractor.cleanupOrphanedFiles();
      }

      const formatHandler = await this.extractor.prepare({
        sourceUrl: routeInfo.url,
        partitioned: isPartitioned,
        bbox,
        format,
        memoryLimitMB: memMB,
        ...callbacks
      });

      if (!await this._checkStorageAvailability(formatHandler, callbacks.onStatus)) {
        this._finishDownload('Cancelled');
        return;
      }

      const formatWarning = formatHandler.getFormatWarning?.();
      if (formatWarning) {
        if (formatWarning.isBlocking) {
          alert(formatWarning.message);
          this._finishDownload('Cancelled');
          return;
        }
        if (!confirm(formatWarning.message + '\n\nContinue anyway?')) {
          this._finishDownload('Cancelled');
          return;
        }
      }

      const baseName = GeoParquetExtractor.getDownloadBaseName(sourceName, bbox);
      await this.extractor.download(formatHandler, {
        baseName, ...callbacks
      });

      this._finishDownload('Complete!');
    } catch (error) {
      if (error.name === 'AbortError' || this.extractor?.cancelled) {
        this._finishDownload('Cancelled');
      } else {
        console.error('Partial download failed:', error);
        this._finishDownload(`Download failed: ${error.message}`, true);
      }
    }
  }

  cancel() {
    this.extractor?.cancel();
    this._updateStatus('Cancelling...');
  }

  // --- Private helpers ---

  /** Returns false if user declines to proceed due to storage constraints. */
  async _checkStorageAvailability(formatHandler, onStatus) {
    const browserUsage = formatHandler.getExpectedBrowserStorageUsage();
    const totalDisk = formatHandler.getTotalExpectedDiskUsage();
    const { usage, quota } = await getStorageEstimate();
    const available = quota - usage;

    onStatus(
      `Browser storage — expected: ${formatSize(browserUsage)}, available: ${formatSize(available)}. Total disk usage: ${formatSize(totalDisk)}`
    );

    if (browserUsage > available) {
      const msg = `Expected browser storage usage (${formatSize(browserUsage)}) exceeds available browser storage (${formatSize(available)}).\nTotal disk usage: ${formatSize(totalDisk)}.\nContinue anyway?`;
      return confirm(msg);
    }
    return true;
  }

  _finishDownload(statusMessage, isError = false) {
    this._setDownloadingState(false);
    this.downloadInfo.innerHTML = '';

    if (isError) {
      this._showError(statusMessage);
    } else {
      this._updateStatus(statusMessage);
      setTimeout(() => this._updateStatus(''), 2000);
    }
  }

  _updateBboxDisplay() {
    if (!this.bboxContainer || !this.map) return;

    const bounds = this.map.getBounds();
    const west = bounds.getWest().toFixed(7);
    const south = bounds.getSouth().toFixed(7);
    const east = bounds.getEast().toFixed(7);
    const north = bounds.getNorth().toFixed(7);

    this.bboxContainer.innerHTML = `<span class="bbox-label">Current bbox:</span> <code>${west},${south},${east},${north}</code>`;
  }

  _setDownloadingState(isDownloading) {
    this.startButton.style.display = isDownloading ? 'none' : '';
    this.cancelButton.style.display = isDownloading ? 'block' : 'none';
    this.progressContainer.style.display = isDownloading ? 'block' : 'none';
    this._lastProgress = 0;
    this._updateProgress(0);
  }

  _updateProgress(percent) {
    if (this.progressBar) {
      if (percent < this._lastProgress) return;
      this._lastProgress = percent;
      this.progressBar.style.width = `${percent}%`;
    }
  }

  _updateStatus(message, isError = false) {
    if (this.statusText) {
      this.statusText.textContent = message;
      const hadErrorClass = this.statusText.classList.contains('error');
      if (isError && !hadErrorClass) {
        this.statusText.classList.add('error');
      } else if (!isError && hadErrorClass) {
        this.statusText.classList.remove('error');
      }
    }
  }

  _showError(message) {
    this._updateStatus(message, true);
  }

  _createFormatRow() {
    const formatRow = document.createElement('div');
    formatRow.className = 'partial-format-row';

    const formatLabel = document.createElement('label');
    formatLabel.textContent = 'Format:';
    formatLabel.className = 'partial-format-label';

    this.formatSelect = document.createElement('select');
    this.formatSelect.className = 'partial-format-select';

    for (const fmt of FORMAT_OPTIONS) {
      const option = document.createElement('option');
      option.value = fmt.value;
      option.textContent = fmt.label;
      this.formatSelect.appendChild(option);
    }

    formatRow.appendChild(formatLabel);
    formatRow.appendChild(this.formatSelect);
    return formatRow;
  }

  _createMemoryRow() {
    const memRow = document.createElement('div');
    memRow.className = 'partial-format-row';

    const memLabel = document.createElement('label');
    memLabel.className = 'partial-format-label';
    memLabel.textContent = 'Memory:';

    this.memorySlider = document.createElement('input');
    this.memorySlider.type = 'range';
    this.memorySlider.className = 'partial-memory-slider';
    this.memorySlider.min = String(MEMORY_MIN_MB);
    this.memorySlider.step = String(MEMORY_STEP);
    this.memorySlider.max = String(getDeviceMaxMemoryMB());
    this.memorySlider.value = String(getDefaultMemoryLimitMB());

    this.memoryValue = document.createElement('span');
    this.memoryValue.className = 'partial-memory-value';
    this.memoryValue.textContent = formatMemory(parseInt(this.memorySlider.value));

    this.memorySlider.addEventListener('input', () => {
      this.memoryValue.textContent = formatMemory(parseInt(this.memorySlider.value));
    });

    memRow.appendChild(memLabel);
    memRow.appendChild(this.memorySlider);
    memRow.appendChild(this.memoryValue);
    return memRow;
  }

  _createButtonsRow() {
    const buttonsRow = document.createElement('div');
    buttonsRow.className = 'partial-buttons-row';

    this.startButton = document.createElement('button');
    this.startButton.className = 'partial-start-btn';
    this.startButton.textContent = 'Start Download';
    this.startButton.addEventListener('click', () => this.startDownload());

    this.cancelButton = document.createElement('button');
    this.cancelButton.className = 'partial-cancel-btn';
    this.cancelButton.textContent = 'Cancel';
    this.cancelButton.style.display = 'none';
    this.cancelButton.addEventListener('click', () => this.cancel());

    buttonsRow.appendChild(this.startButton);
    buttonsRow.appendChild(this.cancelButton);
    return buttonsRow;
  }

  _createProgressContainer() {
    this.progressContainer = document.createElement('div');
    this.progressContainer.className = 'partial-progress-container';
    this.progressContainer.style.display = 'none';

    this.downloadInfo = document.createElement('div');
    this.downloadInfo.className = 'partial-download-info';
    this.progressContainer.appendChild(this.downloadInfo);

    const progressBarOuter = document.createElement('div');
    progressBarOuter.className = 'partial-progress-bar-outer';

    this.progressBar = document.createElement('div');
    this.progressBar.className = 'partial-progress-bar-inner';
    this.progressBar.style.width = '0%';

    progressBarOuter.appendChild(this.progressBar);
    this.progressContainer.appendChild(progressBarOuter);

    this.statusText = document.createElement('div');
    this.statusText.className = 'partial-status-text';
    this.progressContainer.appendChild(this.statusText);

    return this.progressContainer;
  }
}
