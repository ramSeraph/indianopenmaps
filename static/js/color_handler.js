// Color management for vector sources
export class ColorHandler {
  constructor() {
    this.LIGHT = 'light';
    this.DARK = 'dark';

    this.colorLists = {};

    this.colorLists[this.LIGHT] = [
      'FC49A3', 'CC66FF', '66CCFF', '66FFCC', '00FF00', 'FFCC66',
      'FF6666', 'FF0000', 'FF8000', 'FFFF66', '00FFFF',
    ];

    this.colorLists[this.DARK] = [
      'C71585', '663399', '4682B4', '7B68EE', '228B22', 'DAA520',
      'D2691E', 'B22222', 'FF8C00', 'FFD700', '003366',
    ];

    this.availableColors = {};
    for (const [key, colorList] of Object.entries(this.colorLists)) {
      this.availableColors[key] = [...colorList];
    }

    // Session color memory: maps source path -> color index
    this.sessionColorMap = this._loadSessionColors();
  }

  _loadSessionColors() {
    try {
      const stored = sessionStorage.getItem('layerColorAssignments');
      return stored ? JSON.parse(stored) : {};
    } catch (e) {
      return {};
    }
  }

  _saveSessionColors() {
    try {
      sessionStorage.setItem('layerColorAssignments', JSON.stringify(this.sessionColorMap));
    } catch (e) {
      // Ignore storage errors
    }
  }

  minColorLength() {
    return Math.min(...Object.values(this.colorLists).map(list => list.length));
  }

  hasAvailableColors() {
    const key = Object.keys(this.colorLists)[0];
    return this.availableColors[key].length > 0;
  }

  assignColors(sourcePath = null, getActivePaths = null) {
    // Check if we have a remembered color for this source
    if (sourcePath && this.sessionColorMap[sourcePath] !== undefined) {
      const colorIndex = this.sessionColorMap[sourcePath];
      const assigned = {};
      
      for (const key of Object.keys(this.colorLists)) {
        const color = this.colorLists[key][colorIndex];
        // Remove from available if present
        const idx = this.availableColors[key].indexOf(color);
        if (idx !== -1) {
          this.availableColors[key].splice(idx, 1);
          assigned[key] = color;
        } else {
          // Color already in use, fall back to next available
          break;
        }
      }
      
      // If we successfully assigned all color types, return
      if (Object.keys(assigned).length === Object.keys(this.colorLists).length) {
        return assigned;
      }
      
      // Otherwise restore what we took and fall through to normal assignment
      for (const key of Object.keys(assigned)) {
        this.availableColors[key].push(assigned[key]);
      }
    }

    // If no colors available, try to reclaim from inactive sources
    if (!this.hasAvailableColors() && getActivePaths) {
      this._reclaimInactiveColors(getActivePaths());
    }

    if (!this.hasAvailableColors()) {
      throw new Error('No colors available');
    }

    const assigned = {};
    let colorIndex = null;
    
    for (const [key, colorList] of Object.entries(this.colorLists)) {
      const color = this.availableColors[key].shift();
      assigned[key] = color;
      
      // Find the index in the original list for session storage
      if (colorIndex === null) {
        colorIndex = this.colorLists[key].indexOf(color);
      }
    }
    
    // Remember this color assignment for the session
    if (sourcePath && colorIndex !== null) {
      this.sessionColorMap[sourcePath] = colorIndex;
      this._saveSessionColors();
    }
    
    return assigned;
  }

  _reclaimInactiveColors(activePaths) {
    const activeSet = new Set(activePaths);
    const toRemove = [];
    
    for (const [path, colorIndex] of Object.entries(this.sessionColorMap)) {
      if (!activeSet.has(path)) {
        // This source is not active, reclaim its color
        for (const key of Object.keys(this.colorLists)) {
          const color = this.colorLists[key][colorIndex];
          if (!this.availableColors[key].includes(color)) {
            this.availableColors[key].push(color);
          }
        }
        toRemove.push(path);
      }
    }
    
    // Remove reclaimed entries from session map
    for (const path of toRemove) {
      delete this.sessionColorMap[path];
    }
    
    if (toRemove.length > 0) {
      this._saveSessionColors();
    }
  }

  releaseColors(colors) {
    for (const key of Object.keys(this.colorLists)) {
      this.availableColors[key].push(colors[key]);
    }
  }

}
