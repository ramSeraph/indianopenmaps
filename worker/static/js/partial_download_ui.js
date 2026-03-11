// UI for partial (bbox-filtered) downloads
import { PartialDownloadHandler, FORMAT_OPTIONS, getDefaultMemoryLimitMB, getDeviceMaxMemoryMB, MEMORY_STEP, MEMORY_MIN_MB } from './partial_download_handler.js';
import { parquetMetadata } from './parquet_metadata.js';
import { ExtentHandler } from './extent_handler.js';

const FORMAT_LABELS = Object.fromEntries(FORMAT_OPTIONS.map(f => [f.value, f.label]));

export class PartialDownloadUI {
  constructor({ map, routesHandler }) {
    this.map = map;
    this.routesHandler = routesHandler;
    this.selectedSource = null;
    this.extentHandler = new ExtentHandler(map, routesHandler);
    this.partialDownloadHandler = new PartialDownloadHandler();

    // UI elements (created in createSection / createExtentsCheckbox)
    this.extentsContainer = null;
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

    this.extentHandler.addEventListener('loadingchange', (e) => {
      if (this.startButton) this.startButton.disabled = e.detail.loading;
    });

    this.map.on('moveend', () => this._updateBboxDisplay());
  }

  createExtentsCheckbox() {
    this.extentsContainer = this.extentHandler.createCheckbox();
    this.extentsContainer.style.display = 'none';
    return this.extentsContainer;
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

  // --- Public API for DownloadPanelControl ---

  setSourcePath(path) {
    this.selectedSource = path;
    this.extentHandler.setSourcePath(path);
  }

  setVisible(visible) {
    if (this.extentsContainer) this.extentsContainer.style.display = visible ? '' : 'none';
    if (this.section) this.section.style.display = visible ? '' : 'none';
  }

  destroy() {
    this.extentHandler.destroy();
  }

  async startDownload() {
    const sourcePath = this.selectedSource;
    if (!sourcePath) {
      this._showError('Please select a source first');
      return;
    }

    const routes = this.routesHandler.getVectorSources();
    const routeInfo = routes[sourcePath];

    if (!routeInfo || !routeInfo.url) {
      this._showError('No parquet data available for this source');
      return;
    }

    const bounds = this.map.getBounds();
    const bbox = {
      west: bounds.getWest(),
      south: bounds.getSouth(),
      east: bounds.getEast(),
      north: bounds.getNorth()
    };

    const format = this.formatSelect.value;
    const sourceName = routeInfo.name || sourcePath;
    const isPartitioned = routeInfo.partitioned_parquet === true;
    const memMB = parseInt(this.memorySlider.value);
    const memStr = memMB >= 1024 ? `${(memMB / 1024).toFixed(1)} GB` : `${memMB} MB`;

    this._setDownloadingState(true);
    this._updateProgress(0);

    const bboxDisplay = `${bbox.west.toFixed(4)}, ${bbox.south.toFixed(4)} → ${bbox.east.toFixed(4)}, ${bbox.north.toFixed(4)}`;
    this.downloadInfo.innerHTML =
      `<b>${sourceName}</b><br>` +
      `<span class="partial-download-info-detail">format: ${FORMAT_LABELS[format] || format}</span>` +
      `<span class="partial-download-info-detail">bbox: ${bboxDisplay}</span>` +
      `<span class="partial-download-info-detail">memory: ${memStr}</span>`;

    this._updateStatus('Starting download...');

    try {
      let parquetUrl = null;
      let baseUrl = null;
      let filteredPartitions = null;

      if (isPartitioned) {
        const metaUrl = parquetMetadata.getMetaJsonUrl(routeInfo.url);
        baseUrl = parquetMetadata.getBaseUrl(routeInfo.url);

        const metaJson = await parquetMetadata.fetchMetaJson(metaUrl);
        if (metaJson) {
          filteredPartitions = this.partialDownloadHandler.getPartitionsForBbox(metaJson, bbox);

          if (filteredPartitions.length === 0) {
            this._showError('No data found in current bbox');
            this._setDownloadingState(false);
            return;
          }

          this._updateStatus(`Found ${filteredPartitions.length} partition(s) in bbox...`);
        } else {
          this._showError('Could not load partition metadata');
          this._setDownloadingState(false);
          return;
        }
      } else {
        parquetUrl = parquetMetadata.getParquetUrl(routeInfo.url);
      }

      await this.partialDownloadHandler.download({
        sourceName,
        parquetUrl,
        baseUrl,
        partitions: filteredPartitions,
        bbox,
        format,
        memoryLimit: `${this.memorySlider.value}MB`,
        onProgress: (pct) => this._updateProgress(pct),
        onStatus: (msg) => this._updateStatus(msg)
      });

      this._updateProgress(100);
      this._updateStatus('Complete!');

      this._setDownloadingState(false);
      this._updateProgress(0);
      this._updateStatus('');
      this.downloadInfo.innerHTML = '';

    } catch (error) {
      if (error.name !== 'AbortError') {
        console.error('Partial download failed:', error);
        this._showError(`Download failed: ${error.message}`);
      } else {
        this._updateStatus('Cancelled');
        setTimeout(() => this._updateStatus(''), 2000);
      }
      this._setDownloadingState(false);
      this.downloadInfo.innerHTML = '';
    }
  }

  cancel() {
    this.partialDownloadHandler.cancel();
    this._updateStatus('Cancelling after current operation finishes...');
  }

  // --- Private helpers ---

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
    this.startButton.disabled = isDownloading;
    this.cancelButton.style.display = isDownloading ? 'block' : 'none';
    this.progressContainer.style.display = isDownloading ? 'block' : 'none';
    if (this.extentHandler?.checkbox) {
      this.extentHandler.checkbox.disabled = isDownloading;
    }
  }

  _updateProgress(percent) {
    if (this.progressBar) {
      this.progressBar.style.width = `${percent}%`;
    }
  }

  _updateStatus(message) {
    if (this.statusText) {
      this.statusText.textContent = message;
    }
  }

  _showError(message) {
    this._updateStatus(message);
    this.statusText.classList.add('error');
    setTimeout(() => {
      this.statusText.classList.remove('error');
    }, 3000);
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
    const initMB = parseInt(this.memorySlider.value);
    this.memoryValue.textContent = initMB >= 1024 ? `${(initMB / 1024).toFixed(1)} GB` : `${initMB} MB`;

    this.memorySlider.addEventListener('input', () => {
      const mb = parseInt(this.memorySlider.value);
      this.memoryValue.textContent = mb >= 1024 ? `${(mb / 1024).toFixed(1)} GB` : `${mb} MB`;
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
