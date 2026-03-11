// Download panel control for parquet file downloads
import { SizeGetter } from './size_getter.js';
import { parquetMetadata } from './parquet_metadata.js';
import { PartialDownloadUI } from './partial_download_ui.js';

const DEFAULT_LICENSE = '<a href="https://github.com/ramSeraph/indianopenmaps/blob/main/DATA_LICENSE.md" target="_blank" style="color:#4a9eff">CC0 1.0 but attribute datameet and the original government source where possible</a>';

function linkifyUrls(text) {
  if (!text) return '';
  return text.replace(/(https?:\/\/[^\s,)]+)/g, '<a href="$1" target="_blank">$1</a>');
}

const COPY_SVG = '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="9" y="9" width="13" height="13" rx="2"/><path d="M5 15H4a2 2 0 01-2-2V4a2 2 0 012-2h9a2 2 0 012 2v1"/></svg>';
const CHECK_SVG = '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="20 6 9 17 4 12"/></svg>';

export class DownloadPanelControl {
  constructor({ map, routesHandler, vectorSourceHandler }) {
    this.map = map;
    this.container = null;
    this.panelContent = null;
    this.sourceDropdown = null;
    this.linksContainer = null;
    this.noSourcesMessage = null;

    this.routesHandler = routesHandler;
    this.vectorSourceHandler = vectorSourceHandler;
    this.selectedSource = null;
    this.lastCopiedBtn = null;
    this.copyResetTimeout = null;
    this.sizeGetter = new SizeGetter();
    
    this.partialUI = new PartialDownloadUI({
      map,
      routesHandler,
      getSelectedSource: () => this.selectedSource
    });
  }

  _populateSelectedDisplay(container, color, name) {
    container.innerHTML = '';
    if (color) {
      const dot = document.createElement('span');
      dot.className = 'source-color-dot';
      dot.style.backgroundColor = color;
      container.appendChild(dot);
    }
    const nameSpan = document.createElement('span');
    nameSpan.textContent = name;
    container.appendChild(nameSpan);
    const arrow = document.createElement('span');
    arrow.className = 'dropdown-arrow';
    arrow.textContent = '▼';
    container.appendChild(arrow);
  }

  updateSourceDropdown() {
    const handler = this.vectorSourceHandler;

    // Always clear stale extents when the selected source is gone
    if (handler.selectedSourceCount === 0 || (this.selectedSource && !handler.hasSource(this.selectedSource))) {
      this.selectedSource = null;
      this.partialUI.reset();
    }

    if (!this.sourceDropdownContainer) return;

    // Clear dropdown
    this.sourceDropdownContainer.innerHTML = '';
    
    const hasSources = handler.selectedSourceCount > 0;
    this.noSourcesMessage.style.display = hasSources ? 'none' : 'block';
    this.sourceDropdownContainer.style.display = hasSources ? 'block' : 'none';
    this.partialUI.setVisible(hasSources);

    if (!hasSources) {
      this.sourceInfoContainer.innerHTML = '';
      this.linksContainer.innerHTML = '';
      return;
    }

    // Default to first source if nothing selected
    if (!this.selectedSource) {
      this.selectedSource = handler.getSelectedPaths()[0];
    }

    const currentData = handler.getSourceData(this.selectedSource);
    const currentColor = handler.getColorForPath(this.selectedSource);

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
    this._populateSelectedDisplay(selectedDisplay, currentColor, currentData.name);
    
    const optionsList = document.createElement('div');
    optionsList.className = 'dropdown-options';
    optionsList.style.display = 'none';

    // Add options for each selected source
    for (const [path, data] of handler.getSelectedEntries()) {
      const option = document.createElement('div');
      option.className = 'dropdown-option';
      option.dataset.value = path;
      
      const color = handler.getColorForPath(path);
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
        this._populateSelectedDisplay(selectedDisplay, color, data.name);
        optionsList.style.display = 'none';
        this._onSourceSelected(path);
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

    // Load details for current selection
    this._onSourceSelected(this.selectedSource);
  }

  async _onSourceSelected(sourcePath) {
    this.partialUI.setSourcePath(sourcePath);
    this.partialUI.reset();

    const routes = this.routesHandler.getVectorSources();
    const routeInfo = routes[sourcePath];

    if (!routeInfo || !routeInfo.url) {
      this.sourceInfoContainer.innerHTML = '';
      this.linksContainer.innerHTML = '<div class="error-message">No download available for this source</div>';
      return;
    }

    this.updateSourceInfo(routeInfo);
    await this.loadDownloadLinks(routeInfo);
  }

  async loadDownloadLinks(routeInfo) {
    this.linksContainer.innerHTML = '<div class="loading-message">Loading...</div>';

    try {
      const isPartitioned = routeInfo.partitioned_parquet === true;
      let files;

      if (isPartitioned) {
        const metaUrl = parquetMetadata.getMetaJsonUrl(routeInfo.url);
        const baseUrl = parquetMetadata.getBaseUrl(routeInfo.url);
        const partitions = await parquetMetadata.getPartitions(metaUrl);

        if (!partitions || partitions.length === 0) {
          this.linksContainer.innerHTML = '<div class="error-message">No partitions found</div>';
          return;
        }
        files = partitions.map(p => ({ url: baseUrl + p, label: p }));
      } else {
        const parquetUrl = parquetMetadata.getParquetUrl(routeInfo.url);
        files = [{ url: parquetUrl, label: parquetUrl.substring(parquetUrl.lastIndexOf('/') + 1) }];
      }

      this._renderDownloadLinks(files);
    } catch (error) {
      console.error('Error loading download links:', error);
      this.linksContainer.innerHTML = '<div class="error-message">Error loading download links</div>';
    }
  }

  updateSourceInfo(routeInfo) {
    this.sourceInfoContainer.innerHTML = '';

    // Source section
    if (routeInfo.source) {
      const sourceSection = this.createInfoSection('Source', linkifyUrls(routeInfo.source));
      this.sourceInfoContainer.appendChild(sourceSection);
    }

    // Notes section
    if (routeInfo.notes) {
      const notesSection = this.createInfoSection('Notes', routeInfo.notes);
      this.sourceInfoContainer.appendChild(notesSection);
    }

    // License section
    const licenseHtml = routeInfo.license || DEFAULT_LICENSE;
    const licenseSection = this.createInfoSection('License', licenseHtml);
    this.sourceInfoContainer.appendChild(licenseSection);
  }

  createInfoSection(title, contentHtml) {
    const section = document.createElement('div');
    section.className = 'source-info-section collapsed';

    const heading = document.createElement('div');
    heading.className = 'source-info-heading';

    const label = document.createElement('span');
    label.textContent = title;

    const toggleIcon = document.createElement('span');
    toggleIcon.className = 'source-info-toggle';
    toggleIcon.textContent = '▼';

    heading.appendChild(label);
    heading.appendChild(toggleIcon);

    const content = document.createElement('div');
    content.className = 'source-info-content';
    content.innerHTML = contentHtml;

    heading.addEventListener('click', () => {
      section.classList.toggle('collapsed');
    });

    section.appendChild(heading);
    section.appendChild(content);
    return section;
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
    copyBtn.innerHTML = COPY_SVG;
    copyBtn.title = 'Copy URL';
    copyBtn.addEventListener('click', async (e) => {
      e.preventDefault();
      e.stopPropagation();
      try {
        await navigator.clipboard.writeText(url);
        
        if (this.lastCopiedBtn && this.lastCopiedBtn !== copyBtn) {
          this.lastCopiedBtn.innerHTML = COPY_SVG;
        }
        if (this.copyResetTimeout) {
          clearTimeout(this.copyResetTimeout);
        }
        
        copyBtn.innerHTML = CHECK_SVG;
        this.lastCopiedBtn = copyBtn;
        this.copyResetTimeout = setTimeout(() => { 
          copyBtn.innerHTML = COPY_SVG;
          this.lastCopiedBtn = null;
        }, 1500);
      } catch (err) {
        console.error('Failed to copy URL:', err);
      }
    });
    return copyBtn;
  }

  _createLinkItem(url, label) {
    const linkItem = document.createElement('div');
    linkItem.className = 'download-link-item';

    const link = document.createElement('a');
    link.href = url;
    link.target = '_blank';
    link.rel = 'noopener noreferrer';
    link.textContent = label;
    link.className = 'download-link';

    const sizeSpan = document.createElement('span');
    sizeSpan.className = 'file-size loading';

    linkItem.appendChild(link);
    linkItem.appendChild(sizeSpan);
    linkItem.appendChild(this.createCopyButton(url));

    this.sizeGetter.updateElement(url, sizeSpan);
    return linkItem;
  }

  _renderDownloadLinks(files) {
    this.linksContainer.innerHTML = '';

    const listContainer = document.createElement('div');
    listContainer.className = 'partitions-list';

    for (const { url, label } of files) {
      listContainer.appendChild(this._createLinkItem(url, label));
    }

    const heading = isMulti ? `Full Download (${files.length} files)` : 'Full Download';
    const section = this.createDownloadSection(heading, listContainer);
    this.linksContainer.appendChild(section);
  }

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

    // Source info container (notes + license, shown above download links)
    this.sourceInfoContainer = document.createElement('div');
    this.sourceInfoContainer.className = 'source-info-container';

    // Links container
    this.linksContainer = document.createElement('div');
    this.linksContainer.className = 'download-links-container';

    this.panelContent.appendChild(this.noSourcesMessage);
    this.panelContent.appendChild(this.sourceDropdownContainer);
    this.panelContent.appendChild(this.sourceInfoContainer);
    this.panelContent.appendChild(this.linksContainer);

    // Data extents checkbox (between full download and partial download)
    this.panelContent.appendChild(this.partialUI.createExtentsCheckbox());

    // Partial download section
    this.panelContent.appendChild(this.partialUI.createSection());

    this.container.appendChild(this.panelContent);

    // Initial update now that DOM is ready
    this.updateSourceDropdown();

    return this.container;
  }

  onRemove() {
    this.partialUI.destroy();
    this.container?.parentNode?.removeChild(this.container);
    this.map = undefined;
  }
}
