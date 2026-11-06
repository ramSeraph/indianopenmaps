// Color management for vector sources
export class ColorHandler {
  constructor() {
    // Single color list - distinct colors visible on both light and dark backgrounds
    this.colors = [
      'FF3B30', // Red
      '34C759', // Green
      '007AFF', // Blue
      'FF9500', // Orange
      'AF52DE', // Purple
      '5AC8FA', // Cyan
      'FF2D55', // Pink
      'E6B800', // Gold
      '5856D6', // Indigo
      '00C7BE', // Teal
      'FC49A3', // Hot Pink
      'FF5E3A', // Coral
    ];

    this.availableColors = [...this.colors];

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
    return this.colors.length;
  }

  hasAvailableColors() {
    return this.availableColors.length > 0;
  }

  assignColors(sourcePath = null, getActivePaths = null) {
    // Check if we have a remembered color for this source
    if (sourcePath && this.sessionColorMap[sourcePath] !== undefined) {
      const colorIndex = this.sessionColorMap[sourcePath];
      const color = this.colors[colorIndex];
      
      // Remove from available if present
      const idx = this.availableColors.indexOf(color);
      if (idx !== -1) {
        this.availableColors.splice(idx, 1);
        return color;
      }
      // Color already in use, fall through to normal assignment
    }

    // If no colors available, try to reclaim from inactive sources
    if (!this.hasAvailableColors() && getActivePaths) {
      this._reclaimInactiveColors(getActivePaths());
    }

    if (!this.hasAvailableColors()) {
      throw new Error('No colors available');
    }

    const color = this.availableColors.shift();
    const colorIndex = this.colors.indexOf(color);
    
    // Remember this color assignment for the session
    if (sourcePath && colorIndex !== -1) {
      this.sessionColorMap[sourcePath] = colorIndex;
      this._saveSessionColors();
    }
    
    return color;
  }

  _reclaimInactiveColors(activePaths) {
    const activeSet = new Set(activePaths);
    const toRemove = [];
    
    for (const [path, colorIndex] of Object.entries(this.sessionColorMap)) {
      if (!activeSet.has(path)) {
        // This source is not active, reclaim its color
        const color = this.colors[colorIndex];
        if (!this.availableColors.includes(color)) {
          this.availableColors.push(color);
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

  releaseColors(color) {
    this.availableColors.push(color);
  }

}
