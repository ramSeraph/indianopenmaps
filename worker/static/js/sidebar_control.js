// Sidebar control that manages icon buttons and panels
export class SidebarControl {
  constructor() {
    this.map = null;
    this.container = null;
    this.iconsContainer = null;
    this.panelsContainer = null;
    this.panels = new Map(); // id -> { icon, panel, tooltip }
    this.activePanel = null;
  }

  // SVG icons
  static ICONS = {
    search: `<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
      <circle cx="11" cy="11" r="8"/>
      <path d="M21 21l-4.35-4.35"/>
    </svg>`,
    layers: `<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
      <polygon points="12 2 2 7 12 12 22 7 12 2"/>
      <polyline points="2 17 12 22 22 17"/>
      <polyline points="2 12 12 17 22 12"/>
    </svg>`,
    database: `<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
      <ellipse cx="12" cy="5" rx="9" ry="3"/>
      <path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3"/>
      <path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5"/>
    </svg>`,
    download: `<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
      <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/>
      <polyline points="7 10 12 15 17 10"/>
      <line x1="12" y1="15" x2="12" y2="3"/>
    </svg>`
  };

  /**
   * Register a panel with the sidebar
   * @param {string} id - Unique identifier
   * @param {string} iconType - One of: search, layers, database, download
   * @param {string} tooltip - Tooltip text
   * @param {HTMLElement} panelElement - The panel DOM element
   */
  registerPanel(id, iconType, tooltip, panelElement) {
    this.panels.set(id, {
      iconType,
      tooltip,
      panel: panelElement,
      iconBtn: null
    });

    // If already added to map, create the icon button
    if (this.iconsContainer) {
      this._createIconButton(id);
      this._mountPanel(id);
    }
  }

  _createIconButton(id) {
    const panelInfo = this.panels.get(id);
    if (!panelInfo) return;

    const btn = document.createElement('button');
    btn.className = 'sidebar-icon-btn';
    btn.innerHTML = SidebarControl.ICONS[panelInfo.iconType] || '';
    btn.title = panelInfo.tooltip;
    btn.dataset.panelId = id;

    btn.addEventListener('click', () => {
      this.togglePanel(id);
    });

    panelInfo.iconBtn = btn;
    this.iconsContainer.appendChild(btn);
  }

  _mountPanel(id) {
    const panelInfo = this.panels.get(id);
    if (!panelInfo || !panelInfo.panel) return;

    panelInfo.panel.classList.add('sidebar-panel');
    panelInfo.panel.style.display = 'none';
    this.panelsContainer.appendChild(panelInfo.panel);
  }

  togglePanel(id) {
    const panelInfo = this.panels.get(id);
    if (!panelInfo) return;

    if (this.activePanel === id) {
      // Close current panel
      this._closePanel(id);
      this.activePanel = null;
    } else {
      // Close any open panel
      if (this.activePanel) {
        this._closePanel(this.activePanel);
      }
      // Open new panel
      this._openPanel(id);
      this.activePanel = id;
    }
  }

  _openPanel(id) {
    const panelInfo = this.panels.get(id);
    if (!panelInfo) return;

    panelInfo.panel.style.display = 'flex';
    panelInfo.iconBtn?.classList.add('active');
  }

  _closePanel(id) {
    const panelInfo = this.panels.get(id);
    if (!panelInfo) return;

    panelInfo.panel.style.display = 'none';
    panelInfo.iconBtn?.classList.remove('active');
  }

  // Open a specific panel by id
  openPanel(id) {
    if (this.activePanel !== id) {
      this.togglePanel(id);
    }
  }

  // Close currently active panel
  closeActivePanel() {
    if (this.activePanel) {
      this.togglePanel(this.activePanel);
    }
  }

  onAdd(map) {
    this.map = map;

    this.container = document.createElement('div');
    this.container.className = 'maplibregl-ctrl sidebar-container';

    this.iconsContainer = document.createElement('div');
    this.iconsContainer.className = 'sidebar-icons';

    this.panelsContainer = document.createElement('div');
    this.panelsContainer.className = 'sidebar-panels';

    this.container.appendChild(this.iconsContainer);
    this.container.appendChild(this.panelsContainer);

    // Create buttons and mount panels for any already registered
    for (const id of this.panels.keys()) {
      this._createIconButton(id);
      this._mountPanel(id);
    }

    return this.container;
  }

  onRemove() {
    this.container.parentNode?.removeChild(this.container);
    this.map = null;
  }
}
