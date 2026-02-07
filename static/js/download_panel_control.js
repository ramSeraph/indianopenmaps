// Download panel control for parquet file downloads
export class DownloadPanelControl {
  constructor(routesHandler, vectorSourceHandler) {
    this.map = null;
    this.container = null;
    this.panelHeader = null;
    this.panelContent = null;
    this.sourceDropdown = null;
    this.linksContainer = null;
    this.noSourcesMessage = null;

    this.routesHandler = routesHandler;
    this.vectorSourceHandler = vectorSourceHandler;
    this.selectedSource = null;
    this.partitionCache = new Map();
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
      const corsProxyUrl = `/cors-proxy?url=${encodeURIComponent(metaUrl)}`;
      const response = await fetch(corsProxyUrl);
      
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
    copyBtn.textContent = '📋';
    copyBtn.title = 'Copy URL';
    copyBtn.addEventListener('click', async (e) => {
      e.preventDefault();
      e.stopPropagation();
      try {
        await navigator.clipboard.writeText(url);
        copyBtn.textContent = '✓';
        setTimeout(() => { copyBtn.textContent = '📋'; }, 1500);
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
    
    linkItem.appendChild(link);
    linkItem.appendChild(this.createCopyButton(parquetUrl));
    listContainer.appendChild(linkItem);
    
    const section = this.createDownloadSection('Full Download', listContainer);
    this.linksContainer.appendChild(section);
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
      
      linkItem.appendChild(link);
      linkItem.appendChild(this.createCopyButton(partitionUrl));
      listContainer.appendChild(linkItem);
    }

    const section = this.createDownloadSection(`Full Download (${partitions.length} files)`, listContainer);
    this.linksContainer.appendChild(section);
  }

  onAdd(map) {
    this.map = map;

    this.container = document.createElement('div');
    this.container.className = 'maplibregl-ctrl maplibregl-ctrl-download-panel';

    this.panelHeader = document.createElement('div');
    this.panelHeader.className = 'panel-header';

    const headerTitle = document.createElement('h3');
    headerTitle.textContent = 'Download';

    const toggleIcon = document.createElement('span');
    toggleIcon.className = 'toggle-icon';
    toggleIcon.textContent = '▼';

    this.panelHeader.appendChild(headerTitle);
    this.panelHeader.appendChild(toggleIcon);

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

    this.container.appendChild(this.panelHeader);
    this.container.appendChild(this.panelContent);

    this.panelHeader.addEventListener('click', () => {
      this.container.classList.toggle('collapsed');
    });

    // Collapse on mobile by default, open on desktop
    if (window.innerWidth <= 480) {
      this.container.classList.add('collapsed');
    }

    // Initial update
    this.updateSourceDropdown();

    return this.container;
  }

  onRemove() {
    this.container.parentNode.removeChild(this.container);
    this.map = undefined;
  }
}
