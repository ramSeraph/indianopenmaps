// Download panel control for parquet file downloads
import { SizeGetter } from './size_getter.js';

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
    this.lastCopiedBtn = null;
    this.copyResetTimeout = null;
    this.bboxContainer = null;
    this.sizeGetter = new SizeGetter();
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

    // Bbox display container
    this.bboxContainer = document.createElement('div');
    this.bboxContainer.className = 'bbox-display';
    this.panelContent.appendChild(this.bboxContainer);

    this.container.appendChild(this.panelContent);

    return this.container;
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
