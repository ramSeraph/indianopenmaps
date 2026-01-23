// Extracted SourcePanelControl
export class SourcePanelControl {
  constructor(searchParams, routesHandler, vectorSourceHandler) {
    this.map = null;
    this.container = null;
    this.panelHeader = null;
    this.panelContent = null;
    this.searchInput = null;
    this.filterSection = null;
    this.filterTitle = null;
    this.categoryFilters = null;
    this.sourceList = null;
    this.noResults = null;
    this.selectedSourcesList = null;
    this.selectedSourcesContent = null;

    this.searchParams = searchParams;
    this.routesHandler = routesHandler;
    this.vectorSourceHandler = vectorSourceHandler;

    this.allSources = [];
    this.selectedCategories = new Set();
    this.sourcesByCategory = {};
    this.initialSourcesApplied = false;
  }

  initializeCategoryFilters() {
    const categories = Object.keys(this.sourcesByCategory).sort();
    
    this.categoryFilters.innerHTML = categories.map(category => 
      `<div class="category-filter" data-category="${category}">${category}</div>`
    ).join('');
    
    this.categoryFilters.querySelectorAll('.category-filter').forEach(filter => {
      filter.addEventListener('click', () => {
        const category = filter.dataset.category;
        this.toggleCategoryFilter(category);
      });
    });
  }

  toggleCategoryFilter(category) {
    if (this.selectedCategories.has(category)) {
      this.selectedCategories.delete(category);
    } else {
      this.selectedCategories.add(category);
    }
    
    this.categoryFilters.querySelectorAll('.category-filter').forEach(filter => {
      if (filter.dataset.category === category) {
        filter.classList.toggle('active', this.selectedCategories.has(category));
      }
    });
    
    this.renderSourcePanel();
  }

  filterSources() {
    const query = this.searchInput.value.toLowerCase();
    let filtered = this.allSources;
    
    if (this.selectedCategories.size > 0) {
      filtered = filtered.filter(source => {
        for (const selectedCat of this.selectedCategories) {
          if (!source.categories.includes(selectedCat)) {
            return false;
          }
        }
        return true;
      });
    }
    
    if (query) {
      filtered = filtered.filter(source => 
        source.name.toLowerCase().includes(query) || 
        source.path.toLowerCase().includes(query) ||
        source.categories.some(cat => cat.toLowerCase().includes(query))
      );
    }
    
    return filtered;
  }

  renderSourcePanel() {
    const filteredSources = this.filterSources();
    
    this.sourceList.innerHTML = '';
    this.noResults.style.display = filteredSources.length === 0 ? 'block' : 'none';
    
    const sourcesToSelect = [];
    const initialSourcePaths = this.initialSourcesApplied ? [] : this.searchParams.getSourcePaths();
    
    filteredSources.forEach(source => {
      const sourceOption = document.createElement('div');
      sourceOption.className = 'source-option';
      
      const checkbox = document.createElement('input');
      checkbox.type = 'checkbox';
      checkbox.id = `source-${source.path.replace(/\//g, '-')}`;
      checkbox.checked = this.vectorSourceHandler.selectedSources.has(source.path);
      
      checkbox.addEventListener('change', (e) => {
        const sourceInfo = {
          name: source.name,
          path: source.path
        };
        
        if (e.target.checked) {
          if (this.vectorSourceHandler) {
            this.vectorSourceHandler.addVectorSource(sourceInfo);
          }
        } else {
          if (this.vectorSourceHandler) {
            this.vectorSourceHandler.removeVectorSource(source.path);
          }
        }
        this.updateSelectedSourcesList();
      });
      
      const label = document.createElement('label');
      label.htmlFor = checkbox.id;
      label.textContent = source.name;
      
      sourceOption.appendChild(checkbox);
      sourceOption.appendChild(label);
      this.sourceList.appendChild(sourceOption);
      
      if (initialSourcePaths.includes(source.path)) {
        sourcesToSelect.push({ checkbox, source, element: sourceOption });
      }
    });
    
    if (sourcesToSelect.length > 0) {
      for (const item of sourcesToSelect) {
        if (!this.vectorSourceHandler.selectedSources.has(item.source.path)) {
          item.checkbox.checked = true;
          this.vectorSourceHandler.addVectorSource(item.source);
        }
      }
      
      this.initialSourcesApplied = true;
      
      setTimeout(() => {
        sourcesToSelect[0].element.scrollIntoView({ behavior: 'auto', block: 'center' });
      }, 100);
    }
    
    this.updateSelectedSourcesList();
  }

  updateSelectedSourcesList() {
    if (!this.selectedSourcesList) return;
    
    if (this.vectorSourceHandler.selectedSources.size === 0) {
      this.selectedSourcesList.style.display = 'none';
      return;
    }
    
    this.selectedSourcesList.style.display = 'block';
    this.selectedSourcesContent.innerHTML = '';
    
    for (const [path, data] of this.vectorSourceHandler.selectedSources) {
      const item = document.createElement('div');
      item.className = 'selected-source-item';
      
      const nameSpan = document.createElement('span');
      nameSpan.textContent = data.name;
      
      const removeBtn = document.createElement('button');
      removeBtn.className = 'remove-source-btn';
      removeBtn.innerHTML = '×';
      removeBtn.title = 'Remove source';
      removeBtn.addEventListener('click', () => {
        this.vectorSourceHandler.removeVectorSource(path);
        
        const checkbox = this.sourceList.querySelector(`#source-${path.replace(/\//g, '-')}`);
        if (checkbox) checkbox.checked = false;
        this.updateSelectedSourcesList();
      });
      
      item.appendChild(nameSpan);
      item.appendChild(removeBtn);
      this.selectedSourcesContent.appendChild(item);
    }
  }

  async loadAvailableSources() {
    try {
      const routes = this.routesHandler.getVectorSources();
      
      if (routes) {
        this.allSources = [];
        this.sourcesByCategory = {};
        
        for (const [path, info] of Object.entries(routes)) {
          
          const source = {
            path,
            name: info.name || path,
            categories: Array.isArray(info.category) ? info.category : (info.category ? [info.category] : [])
          };
          
          this.allSources.push(source);
          
          for (const cat of source.categories) {
            if (!this.sourcesByCategory[cat]) this.sourcesByCategory[cat] = [];
            this.sourcesByCategory[cat].push(source);
          }
        }
        
        this.allSources.sort((a, b) => a.name.localeCompare(b.name));
        
        this.initializeCategoryFilters();
        this.renderSourcePanel();
      }
    } catch (error) {
      console.error('Error loading available sources:', error);
    }
  }

  onAdd(map) {
    this.map = map;
    
    this.container = document.createElement('div');
    this.container.className = 'maplibregl-ctrl maplibregl-ctrl-source-panel';
    
    this.panelHeader = document.createElement('div');
    this.panelHeader.className = 'panel-header';
    
    const headerTitle = document.createElement('h3');
    headerTitle.textContent = 'Vector Sources';
    
    const toggleIcon = document.createElement('span');
    toggleIcon.className = 'toggle-icon';
    toggleIcon.textContent = '▼';
    
    this.panelHeader.appendChild(headerTitle);
    this.panelHeader.appendChild(toggleIcon);
    
    this.panelContent = document.createElement('div');
    this.panelContent.className = 'panel-content';
    
    const searchBox = document.createElement('div');
    searchBox.className = 'search-box';
    this.searchInput = document.createElement('input');
    this.searchInput.type = 'text';
    this.searchInput.placeholder = 'Search sources...';
    searchBox.appendChild(this.searchInput);
    
    this.selectedSourcesList = document.createElement('div');
    this.selectedSourcesList.className = 'selected-sources-list';
    this.selectedSourcesList.style.display = 'none';
    
    const selectedHeading = document.createElement('div');
    selectedHeading.className = 'selected-sources-heading';
    selectedHeading.textContent = 'Selected Sources';
    
    this.selectedSourcesContent = document.createElement('div');
    this.selectedSourcesContent.className = 'selected-sources-content';
    
    this.selectedSourcesList.appendChild(selectedHeading);
    this.selectedSourcesList.appendChild(this.selectedSourcesContent);
    
    this.filterSection = document.createElement('div');
    this.filterSection.className = 'filter-section collapsed';
    
    this.filterTitle = document.createElement('div');
    this.filterTitle.className = 'filter-title';
    
    const filterLabel = document.createElement('span');
    filterLabel.textContent = 'Filter by category:';
    
    const filterToggle = document.createElement('span');
    filterToggle.className = 'filter-toggle-icon';
    filterToggle.textContent = '▼';
    
    this.filterTitle.appendChild(filterLabel);
    this.filterTitle.appendChild(filterToggle);
    
    this.categoryFilters = document.createElement('div');
    this.categoryFilters.className = 'category-filters';
    
    this.filterSection.appendChild(this.filterTitle);
    this.filterSection.appendChild(this.categoryFilters);
    
    const layersHeading = document.createElement('div');
    layersHeading.className = 'layers-heading';
    layersHeading.textContent = 'Available Sources';
    
    this.sourceList = document.createElement('div');
    this.sourceList.className = 'source-list';
    
    this.noResults = document.createElement('div');
    this.noResults.className = 'no-results';
    this.noResults.textContent = 'No sources found';
    this.noResults.style.display = 'none';
    
    this.panelContent.appendChild(searchBox);
    this.panelContent.appendChild(this.selectedSourcesList);
    this.panelContent.appendChild(this.filterSection);
    this.panelContent.appendChild(layersHeading);
    this.panelContent.appendChild(this.sourceList);
    this.panelContent.appendChild(this.noResults);
    
    this.container.appendChild(this.panelHeader);
    this.container.appendChild(this.panelContent);
    
    this.panelHeader.addEventListener('click', () => {
      this.container.classList.toggle('collapsed');
    });
    
    // Collapse panel on mobile by default
    if (window.innerWidth <= 480) {
      this.container.classList.add('collapsed');
    }
    
    this.filterTitle.addEventListener('click', () => {
      this.filterSection.classList.toggle('collapsed');
    });
    
    this.searchInput.addEventListener('input', () => {
      this.renderSourcePanel();
    });
    
    return this.container;
  }

  onRemove() {
    this.container.parentNode.removeChild(this.container);
    this.map = undefined;
  }
}
