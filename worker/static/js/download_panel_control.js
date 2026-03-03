// Download panel control for parquet file downloads
import { SizeGetter } from './size_getter.js';
import { PartialDownloadHandler, getDefaultMemoryLimitMB, getDeviceMaxMemoryMB, MEMORY_STEP, MEMORY_MIN_MB } from './partial_download_handler.js';

export class DownloadPanelControl {
  constructor(routesHandler, vectorSourceHandler) {
    this.map = null;
    this.container = null;
    this.panelContent = null;
    this.sourceDropdown = null;
    this.linksContainer = null;
    this.noSourcesMessage = null;

    this.routesHandler = routesHandler;
    this.vectorSourceHandler = vectorSourceHandler;
    this.selectedSource = null;
    this.partitionCache = new Map();
    this.metaJsonCache = new Map();
    this.lastCopiedBtn = null;
    this.copyResetTimeout = null;
    this.bboxContainer = null;
    this.sizeGetter = new SizeGetter();
    
    // Partial download state
    this.partialDownloadHandler = new PartialDownloadHandler();
    this.partialDownloadSection = null;
    this.formatSelect = null;
    this.startButton = null;
    this.cancelButton = null;
    this.progressContainer = null;
    this.progressBar = null;
    this.statusText = null;
  }

  getParquetUrl(originalUrl) {
    // Replace .mosaic.json or .pmtiles with .parquet
    return originalUrl.replace(/\.(mosaic\.json|pmtiles)$/, '.parquet');
  }

  getMetaJsonUrl(originalUrl) {
    // Replace .mosaic.json or .pmtiles with .parquet.meta.json
    return originalUrl.replace(/\.(mosaic\.json|pmtiles)$/, '.parquet.meta.json');
  }

  getBaseUrl(originalUrl) {
    // Get base URL (directory) from original URL
    const lastSlash = originalUrl.lastIndexOf('/');
    return originalUrl.substring(0, lastSlash + 1);
  }

  async fetchPartitions(metaUrl) {
    if (this.partitionCache.has(metaUrl)) {
      return this.partitionCache.get(metaUrl);
    }

    try {
      const proxyUrl = `/proxy?url=${encodeURIComponent(metaUrl)}`;
      const response = await fetch(proxyUrl);
      
      if (!response.ok) {
        throw new Error(`Failed to fetch meta.json: ${response.status}`);
      }
      
      const metaJson = await response.json();
      // Cache the full meta.json for partial download bbox filtering
      this.metaJsonCache.set(metaUrl, metaJson);
      // Partitions are the keys in the extents object
      const partitions = metaJson.extents ? Object.keys(metaJson.extents) : [];
      this.partitionCache.set(metaUrl, partitions);
      return partitions;
    } catch (error) {
      console.error('Error fetching partition metadata:', error);
      return null;
    }
  }

  updateSourceDropdown() {
    if (!this.sourceDropdownContainer) return;

    const selectedSources = this.vectorSourceHandler.selectedSources;
    
    // Clear dropdown
    this.sourceDropdownContainer.innerHTML = '';
    
    if (selectedSources.size === 0) {
      this.noSourcesMessage.style.display = 'block';
      this.sourceDropdownContainer.style.display = 'none';
      this.linksContainer.innerHTML = '';
      this.selectedSource = null;
      return;
    }

    this.noSourcesMessage.style.display = 'none';
    this.sourceDropdownContainer.style.display = 'block';

    // If current selection is no longer valid, reset it
    if (this.selectedSource && !selectedSources.has(this.selectedSource)) {
      this.selectedSource = null;
    }

    // Default to first source if nothing selected
    if (!this.selectedSource) {
      this.selectedSource = selectedSources.keys().next().value;
    }

    const currentData = selectedSources.get(this.selectedSource);
    const currentColor = this.vectorSourceHandler.getColorForPath(this.selectedSource);

    // Add label above dropdown
    const dropdownLabel = document.createElement('div');
    dropdownLabel.className = 'dropdown-label';
    dropdownLabel.textContent = 'Select a source...';
    this.sourceDropdownContainer.appendChild(dropdownLabel);

    // Create custom dropdown
    const dropdownWrapper = document.createElement('div');
    dropdownWrapper.className = 'custom-dropdown';
    
    const selectedDisplay = document.createElement('div');
    selectedDisplay.className = 'dropdown-selected';
    
    // Show current selection
    if (currentColor) {
      const dot = document.createElement('span');
      dot.className = 'source-color-dot';
      dot.style.backgroundColor = currentColor;
      selectedDisplay.appendChild(dot);
    }
    const name = document.createElement('span');
    name.textContent = currentData.name;
    selectedDisplay.appendChild(name);
    const arrow = document.createElement('span');
    arrow.className = 'dropdown-arrow';
    arrow.textContent = '▼';
    selectedDisplay.appendChild(arrow);
    
    const optionsList = document.createElement('div');
    optionsList.className = 'dropdown-options';
    optionsList.style.display = 'none';

    // Add options for each selected source
    for (const [path, data] of selectedSources) {
      const option = document.createElement('div');
      option.className = 'dropdown-option';
      option.dataset.value = path;
      
      const color = this.vectorSourceHandler.getColorForPath(path);
      if (color) {
        const colorDot = document.createElement('span');
        colorDot.className = 'source-color-dot';
        colorDot.style.backgroundColor = color;
        option.appendChild(colorDot);
      }
      
      const nameSpan = document.createElement('span');
      nameSpan.textContent = data.name;
      option.appendChild(nameSpan);
      
      option.addEventListener('click', () => {
        this.selectedSource = path;
        // Update display
        selectedDisplay.innerHTML = '';
        if (color) {
          const dot = document.createElement('span');
          dot.className = 'source-color-dot';
          dot.style.backgroundColor = color;
          selectedDisplay.appendChild(dot);
        }
        const name = document.createElement('span');
        name.textContent = data.name;
        selectedDisplay.appendChild(name);
        const arrow = document.createElement('span');
        arrow.className = 'dropdown-arrow';
        arrow.textContent = '▼';
        selectedDisplay.appendChild(arrow);
        
        optionsList.style.display = 'none';
        this.loadDownloadLinks(path);
      });
      
      optionsList.appendChild(option);
    }

    selectedDisplay.addEventListener('click', (e) => {
      e.stopPropagation();
      optionsList.style.display = optionsList.style.display === 'none' ? 'block' : 'none';
    });

    // Close dropdown when clicking outside
    document.addEventListener('click', () => {
      optionsList.style.display = 'none';
    });

    dropdownWrapper.appendChild(selectedDisplay);
    dropdownWrapper.appendChild(optionsList);
    this.sourceDropdownContainer.appendChild(dropdownWrapper);

    // Load links for current selection
    this.loadDownloadLinks(this.selectedSource);
  }

  async loadDownloadLinks(sourcePath) {
    this.linksContainer.innerHTML = '<div class="loading-message">Loading...</div>';

    try {
      const routes = this.routesHandler.getVectorSources();
      const routeInfo = routes[sourcePath];

      if (!routeInfo || !routeInfo.url) {
        this.linksContainer.innerHTML = '<div class="error-message">No download available for this source</div>';
        return;
      }

      const isPartitioned = routeInfo.partitioned_parquet === true;

      if (isPartitioned) {
        await this.loadPartitionedLinks(routeInfo.url, routeInfo.name || sourcePath);
      } else {
        this.loadSingleLink(routeInfo.url, routeInfo.name || sourcePath);
      }
    } catch (error) {
      console.error('Error loading download links:', error);
      this.linksContainer.innerHTML = '<div class="error-message">Error loading download links</div>';
    }
  }

  createDownloadSection(headingText, listContainer) {
    const section = document.createElement('div');
    section.className = 'download-section';
    
    const heading = document.createElement('div');
    heading.className = 'download-section-heading';
    
    const leftPart = document.createElement('div');
    leftPart.className = 'download-section-left';
    
    const label = document.createElement('span');
    label.textContent = headingText;
    
    const helpBtn = document.createElement('a');
    helpBtn.href = '/data-help#geoparquet';
    helpBtn.target = '_blank';
    helpBtn.className = 'download-help-btn';
    helpBtn.textContent = '?';
    helpBtn.title = 'Help with downloaded data';
    helpBtn.addEventListener('click', (e) => {
      e.stopPropagation();
    });
    
    leftPart.appendChild(label);
    leftPart.appendChild(helpBtn);
    
    const toggleIcon = document.createElement('span');
    toggleIcon.className = 'download-toggle-icon';
    toggleIcon.textContent = '▼';
    
    heading.appendChild(leftPart);
    heading.appendChild(toggleIcon);
    
    heading.addEventListener('click', () => {
      section.classList.toggle('collapsed');
    });
    
    section.appendChild(heading);
    section.appendChild(listContainer);
    
    return section;
  }

  createCopyButton(url) {
    const copyBtn = document.createElement('button');
    copyBtn.className = 'copy-url-btn';
    // Two overlapping squares - standard copy icon
    copyBtn.innerHTML = '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="9" y="9" width="13" height="13" rx="2"/><path d="M5 15H4a2 2 0 01-2-2V4a2 2 0 012-2h9a2 2 0 012 2v1"/></svg>';
    copyBtn.title = 'Copy URL';
    copyBtn.addEventListener('click', async (e) => {
      e.preventDefault();
      e.stopPropagation();
      try {
        await navigator.clipboard.writeText(url);
        
        // Reset previous copied button if exists
        if (this.lastCopiedBtn && this.lastCopiedBtn !== copyBtn) {
          this.lastCopiedBtn.innerHTML = '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="9" y="9" width="13" height="13" rx="2"/><path d="M5 15H4a2 2 0 01-2-2V4a2 2 0 012-2h9a2 2 0 012 2v1"/></svg>';
        }
        if (this.copyResetTimeout) {
          clearTimeout(this.copyResetTimeout);
        }
        
        copyBtn.innerHTML = '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="20 6 9 17 4 12"/></svg>';
        this.lastCopiedBtn = copyBtn;
        this.copyResetTimeout = setTimeout(() => { 
          copyBtn.innerHTML = '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="9" y="9" width="13" height="13" rx="2"/><path d="M5 15H4a2 2 0 01-2-2V4a2 2 0 012-2h9a2 2 0 012 2v1"/></svg>';
          this.lastCopiedBtn = null;
        }, 1500);
      } catch (err) {
        console.error('Failed to copy URL:', err);
      }
    });
    return copyBtn;
  }

  loadSingleLink(originalUrl, name) {
    const parquetUrl = this.getParquetUrl(originalUrl);
    // Extract filename from URL
    const filename = parquetUrl.substring(parquetUrl.lastIndexOf('/') + 1);
    
    this.linksContainer.innerHTML = '';

    const listContainer = document.createElement('div');
    listContainer.className = 'partitions-list';
    
    const linkItem = document.createElement('div');
    linkItem.className = 'download-link-item';
    
    const link = document.createElement('a');
    link.href = parquetUrl;
    link.target = '_blank';
    link.rel = 'noopener noreferrer';
    link.textContent = filename;
    link.className = 'download-link';
    
    const sizeSpan = document.createElement('span');
    sizeSpan.className = 'file-size loading';
    
    linkItem.appendChild(link);
    linkItem.appendChild(sizeSpan);
    linkItem.appendChild(this.createCopyButton(parquetUrl));
    listContainer.appendChild(linkItem);
    
    const section = this.createDownloadSection('Full Download', listContainer);
    this.linksContainer.appendChild(section);

    // Fetch file size asynchronously
    this.sizeGetter.updateElement(parquetUrl, sizeSpan);
  }

  async loadPartitionedLinks(originalUrl, name) {
    const metaUrl = this.getMetaJsonUrl(originalUrl);
    const baseUrl = this.getBaseUrl(originalUrl);
    
    const partitions = await this.fetchPartitions(metaUrl);
    
    if (!partitions || partitions.length === 0) {
      this.linksContainer.innerHTML = '<div class="error-message">No partitions found</div>';
      return;
    }

    this.linksContainer.innerHTML = '';

    const listContainer = document.createElement('div');
    listContainer.className = 'partitions-list';

    const sizeElements = [];

    for (const partition of partitions) {
      const partitionUrl = baseUrl + partition;
      
      const linkItem = document.createElement('div');
      linkItem.className = 'download-link-item';
      
      const link = document.createElement('a');
      link.href = partitionUrl;
      link.target = '_blank';
      link.rel = 'noopener noreferrer';
      link.textContent = partition;
      link.className = 'download-link partition-link';
      
      const sizeSpan = document.createElement('span');
      sizeSpan.className = 'file-size loading';
      
      linkItem.appendChild(link);
      linkItem.appendChild(sizeSpan);
      linkItem.appendChild(this.createCopyButton(partitionUrl));
      listContainer.appendChild(linkItem);

      sizeElements.push({ url: partitionUrl, element: sizeSpan });
    }

    const section = this.createDownloadSection(`Full Download (${partitions.length} files)`, listContainer);
    this.linksContainer.appendChild(section);

    // Fetch file sizes asynchronously
    for (const { url, element } of sizeElements) {
      this.sizeGetter.updateElement(url, element);
    }
  }

  updateBboxDisplay() {
    if (!this.bboxContainer || !this.map) return;
    
    const bounds = this.map.getBounds();
    const west = bounds.getWest().toFixed(7);
    const south = bounds.getSouth().toFixed(7);
    const east = bounds.getEast().toFixed(7);
    const north = bounds.getNorth().toFixed(7);
    
    this.bboxContainer.innerHTML = `<span class="bbox-label">Current bbox:</span> <code>${west},${south},${east},${north}</code>`;
  }

  /**
   * Create the panel element for sidebar mounting
   * @returns {HTMLElement} The panel element
   */
  createPanel() {
    this.container = document.createElement('div');
    this.container.className = 'maplibregl-ctrl-download-panel';

    this.panelContent = document.createElement('div');
    this.panelContent.className = 'panel-content';

    // No sources message
    this.noSourcesMessage = document.createElement('div');
    this.noSourcesMessage.className = 'no-sources-message';
    this.noSourcesMessage.textContent = 'Select sources in Vector Sources panel to enable downloads';

    // Source dropdown container
    this.sourceDropdownContainer = document.createElement('div');
    this.sourceDropdownContainer.className = 'download-source-dropdown-container';
    this.sourceDropdownContainer.style.display = 'none';

    // Links container
    this.linksContainer = document.createElement('div');
    this.linksContainer.className = 'download-links-container';

    this.panelContent.appendChild(this.noSourcesMessage);
    this.panelContent.appendChild(this.sourceDropdownContainer);
    this.panelContent.appendChild(this.linksContainer);

    // Partial download section
    this.partialDownloadSection = this.createPartialDownloadSection();
    this.panelContent.appendChild(this.partialDownloadSection);

    this.container.appendChild(this.panelContent);

    return this.container;
  }

  /**
   * Create the partial download section with bbox, format selector, buttons, and progress
   */
  createPartialDownloadSection() {
    const section = document.createElement('div');
    section.className = 'download-section partial-download-section';

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
      section.classList.toggle('collapsed');
    });
    
    section.appendChild(heading);

    // Content container
    const content = document.createElement('div');
    content.className = 'partial-download-content';

    // Bbox display (moved here from bottom)
    this.bboxContainer = document.createElement('div');
    this.bboxContainer.className = 'bbox-display partial-bbox';
    content.appendChild(this.bboxContainer);

    // Format selector row
    const formatRow = document.createElement('div');
    formatRow.className = 'partial-format-row';
    
    const formatLabel = document.createElement('label');
    formatLabel.textContent = 'Format:';
    formatLabel.className = 'partial-format-label';
    
    this.formatSelect = document.createElement('select');
    this.formatSelect.className = 'partial-format-select';
    
    const formats = [
      { value: 'geojson', label: 'GeoJSON' },
      { value: 'geojsonseq', label: 'GeoJSONSeq (.geojsonl)' },
      { value: 'geoparquet', label: 'GeoParquet (v1.1.0)' },
      { value: 'csv', label: 'CSV (WKT geometry)' }
    ];
    
    for (const fmt of formats) {
      const option = document.createElement('option');
      option.value = fmt.value;
      option.textContent = fmt.label;
      this.formatSelect.appendChild(option);
    }
    
    formatRow.appendChild(formatLabel);
    formatRow.appendChild(this.formatSelect);
    content.appendChild(formatRow);

    // Memory limit slider row
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
    content.appendChild(memRow);

    // Buttons row
    const buttonsRow = document.createElement('div');
    buttonsRow.className = 'partial-buttons-row';
    
    this.startButton = document.createElement('button');
    this.startButton.className = 'partial-start-btn';
    this.startButton.textContent = 'Start Download';
    this.startButton.addEventListener('click', () => this.startPartialDownload());
    
    this.cancelButton = document.createElement('button');
    this.cancelButton.className = 'partial-cancel-btn';
    this.cancelButton.textContent = 'Cancel';
    this.cancelButton.style.display = 'none';
    this.cancelButton.addEventListener('click', () => this.cancelPartialDownload());
    
    buttonsRow.appendChild(this.startButton);
    buttonsRow.appendChild(this.cancelButton);
    content.appendChild(buttonsRow);

    // Progress container
    this.progressContainer = document.createElement('div');
    this.progressContainer.className = 'partial-progress-container';
    this.progressContainer.style.display = 'none';

    // Download info summary (shown during download)
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
    
    content.appendChild(this.progressContainer);

    section.appendChild(content);
    return section;
  }

  /**
   * Start partial download for current source and bbox
   */
  async startPartialDownload() {
    if (!this.selectedSource || !this.map) {
      this.showError('Please select a source first');
      return;
    }

    const routes = this.routesHandler.getVectorSources();
    const routeInfo = routes[this.selectedSource];
    
    if (!routeInfo || !routeInfo.url) {
      this.showError('No parquet data available for this source');
      return;
    }

    // Capture current bbox
    const bounds = this.map.getBounds();
    const bbox = {
      west: bounds.getWest(),
      south: bounds.getSouth(),
      east: bounds.getEast(),
      north: bounds.getNorth()
    };

    const format = this.formatSelect.value;
    const sourceName = routeInfo.name || this.selectedSource;

    // Show save file picker IMMEDIATELY (must be in user gesture context)
    let userFileHandle;
    try {
      userFileHandle = await this.partialDownloadHandler.promptSaveFile(sourceName, bbox, format);
    } catch (e) {
      if (e.name === 'AbortError') {
        return; // User cancelled picker
      }
      this.showError(`Failed to open save dialog: ${e.message}`);
      return;
    }

    const isPartitioned = routeInfo.partitioned_parquet === true;
    const memMB = parseInt(this.memorySlider.value);
    const memStr = memMB >= 1024 ? `${(memMB / 1024).toFixed(1)} GB` : `${memMB} MB`;
    const formatLabels = { geojson: 'GeoJSON', geojsonseq: 'GeoJSONSeq', geoparquet: 'GeoParquet', csv: 'CSV' };

    // Update UI state
    this.setDownloadingState(true);
    this.updateProgress(0);

    // Show download info summary
    const bboxDisplay = `${bbox.west.toFixed(4)}, ${bbox.south.toFixed(4)} → ${bbox.east.toFixed(4)}, ${bbox.north.toFixed(4)}`;
    this.downloadInfo.innerHTML = 
      `<b>${sourceName}</b><br>` +
      `<span class="partial-download-info-detail">format: ${formatLabels[format] || format}</span>` +
      `<span class="partial-download-info-detail">bbox: ${bboxDisplay}</span>` +
      `<span class="partial-download-info-detail">memory: ${memStr}</span>`;

    this.updateStatus(`Starting download...`);

    try {
      let parquetUrl = null;
      let baseUrl = null;
      let filteredPartitions = null;

      if (isPartitioned) {
        // Get partitions filtered by bbox
        const metaUrl = this.getMetaJsonUrl(routeInfo.url);
        baseUrl = this.getBaseUrl(routeInfo.url);
        
        // Ensure we have the meta.json cached
        if (!this.metaJsonCache.has(metaUrl)) {
          await this.fetchPartitions(metaUrl);
        }
        
        const metaJson = this.metaJsonCache.get(metaUrl);
        if (metaJson) {
          filteredPartitions = this.partialDownloadHandler.getPartitionsForBbox(metaJson, bbox);
          
          if (filteredPartitions.length === 0) {
            this.showError('No data found in current bbox');
            this.setDownloadingState(false);
            return;
          }
          
          this.updateStatus(`Found ${filteredPartitions.length} partition(s) in bbox...`);
        } else {
          this.showError('Could not load partition metadata');
          this.setDownloadingState(false);
          return;
        }
      } else {
        // Single parquet file
        parquetUrl = this.getParquetUrl(routeInfo.url);
      }

      await this.partialDownloadHandler.download({
        sourceName,
        parquetUrl,
        baseUrl,
        partitions: filteredPartitions,
        bbox,
        format,
        userFileHandle,
        memoryLimit: `${this.memorySlider.value}MB`,
        onProgress: (pct) => this.updateProgress(pct),
        onStatus: (msg) => this.updateStatus(msg)
      });

      // Wait for progress bar CSS transition to 100% before showing alert
      this.updateProgress(100);
      this.updateStatus('Complete!');
      const fileName = userFileHandle.name;
      await new Promise(r => {
        this.progressBar.addEventListener('transitionend', r, { once: true });
      });
      alert(`Download complete!\n\nSaved to: ${fileName}`);

      this.setDownloadingState(false);
      this.updateProgress(0);
      this.updateStatus('');
      this.downloadInfo.innerHTML = '';

    } catch (error) {
      if (error.name !== 'AbortError') {
        console.error('Partial download failed:', error);
        this.showError(`Download failed: ${error.message}`);
      } else {
        this.updateStatus('Cancelled');
        setTimeout(() => this.updateStatus(''), 2000);
      }
      this.setDownloadingState(false);
      this.downloadInfo.innerHTML = '';
    }
  }

  /**
   * Cancel ongoing partial download
   */
  cancelPartialDownload() {
    this.partialDownloadHandler.cancel();
    this.updateStatus('Cancelling after current operation finishes...');
  }

  /**
   * Update UI for downloading/idle state
   */
  setDownloadingState(isDownloading) {
    this.startButton.disabled = isDownloading;
    this.cancelButton.style.display = isDownloading ? 'block' : 'none';
    this.progressContainer.style.display = isDownloading ? 'block' : 'none';
  }

  /**
   * Update progress bar
   */
  updateProgress(percent) {
    if (this.progressBar) {
      this.progressBar.style.width = `${percent}%`;
    }
  }

  /**
   * Update status text
   */
  updateStatus(message) {
    if (this.statusText) {
      this.statusText.textContent = message;
    }
  }

  /**
   * Show error message
   */
  showError(message) {
    this.updateStatus(message);
    this.statusText.classList.add('error');
    setTimeout(() => {
      this.statusText.classList.remove('error');
    }, 3000);
  }

  /**
   * Set the map reference and initialize map-dependent features
   */
  setMap(map) {
    this.map = map;
    
    // Update bbox on map move
    this.map.on('moveend', () => this.updateBboxDisplay());
    
    // Initial bbox update
    this.updateBboxDisplay();
    
    // Initial dropdown update
    this.updateSourceDropdown();
  }

  onRemove() {
    this.container?.parentNode?.removeChild(this.container);
    this.map = undefined;
  }
}
