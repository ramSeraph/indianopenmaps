// URL parameter handler for viewer
export class SearchParamHandler {
  constructor() {
    this.queryParams = new URLSearchParams(window.location.search);
    this.hashParams = new URLSearchParams(window.location.hash.substring(1));
  }

  // Query params (markerLat, markerLon)
  getQueryParam(name) {
    return this.queryParams.get(name);
  }

  getQueryParamFloat(name) {
    const value = this.getQueryParam(name);
    return value ? parseFloat(value) : null;
  }

  // Hash params (base, terrain, source)
  getHashParam(name) {
    return this.hashParams.get(name);
  }

  getHashParamArray(name, separator = ',') {
    const value = this.getHashParam(name);
    return value ? value.split(separator) : [];
  }

  updateHashParam(paramName, paramValue) {
    const currentHash = window.location.hash;
    const urlParams = new URLSearchParams(currentHash.substring(1));
    urlParams.set(paramName, paramValue);
    const newHash = '#' + urlParams.toString().replaceAll('%2F', '/');
    window.location.hash = newHash;
  }

  // Convenience methods for common parameters
  getMarkerLat() {
    return this.getQueryParamFloat('markerLat');
  }

  getMarkerLon() {
    return this.getQueryParamFloat('markerLon');
  }

  getBaseLayer(defaultValue) {
    return this.getHashParam('base') || defaultValue;
  }

  getTerrain(defaultValue = 'false') {
    return this.getHashParam('terrain') || defaultValue;
  }

  getSourcePaths() {
    return this.getHashParamArray('source');
  }

  updateBaseLayer(layerName) {
    this.updateHashParam('base', layerName);
  }

  updateTerrain(enabled) {
    this.updateHashParam('terrain', enabled ? 'true' : 'false');
  }

  updateSources(sourcePaths) {
    const value = Array.isArray(sourcePaths) ? sourcePaths.join(',') : sourcePaths;
    this.updateHashParam('source', value || '');
  }
}
