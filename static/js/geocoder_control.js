import * as maplibregl from 'https://esm.sh/maplibre-gl@5.6.2';

let geocoderMarker = null;

export class GeocoderControl {
  constructor() {
    this.map = null;
    this.container = null;
    this.input = null;
    this.resultsEl = null;
    this.timeout = null;
  }

  async geocodeSearch(query) {
    if (!query || query.trim().length < 3) {
      this.resultsEl.style.display = 'none';
      return;
    }
    this.resultsEl.style.display = 'block';
    this.resultsEl.innerHTML = '<div class="geocoder-loading">Searching...</div>';
    try {
      const response = await fetch(`https://nominatim.openstreetmap.org/search?format=json&q=${encodeURIComponent(query)}&countrycodes=in&limit=5`);
      const results = await response.json();
      if (results.length === 0) {
        this.resultsEl.innerHTML = '<div class="geocoder-loading">No results found</div>';
        return;
      }
      this.resultsEl.innerHTML = results.map(result => {
        return `<div class="geocoder-result" data-lat="${result.lat}" data-lon="${result.lon}">${result.display_name}</div>`;
      }).join('');
      this.resultsEl.querySelectorAll('.geocoder-result').forEach(resultEl => {
        resultEl.addEventListener('click', () => {
          const lat = parseFloat(resultEl.dataset.lat);
          const lon = parseFloat(resultEl.dataset.lon);
          this.flyToLocation(lat, lon, resultEl.textContent);
          this.resultsEl.style.display = 'none';
          this.input.value = resultEl.textContent;
        });
      });
    } catch (error) {
      console.error('Geocoding error:', error);
      this.resultsEl.innerHTML = '<div class="geocoder-loading">Error searching</div>';
    }
  }

  flyToLocation(lat, lon, name) {
    this.map.flyTo({ center: [lon, lat], zoom: 14 });
    if (geocoderMarker) geocoderMarker.remove();
    geocoderMarker = new maplibregl.Marker({ color: '#FF5733' })
      .setLngLat([lon, lat])
      .setPopup(new maplibregl.Popup().setHTML(`<strong>${name}</strong>`))
      .addTo(this.map)
      .togglePopup();
  }

  onAdd(map) {
    this.map = map;
    this.container = document.createElement('div');
    this.container.className = 'maplibregl-ctrl maplibregl-ctrl-group maplibregl-ctrl-geocoder geocoder-control';
    
    this.inputWrapper = document.createElement('div');
    this.inputWrapper.className = 'geocoder-input-wrapper';
    
    this.input = document.createElement('input');
    this.input.type = 'text';
    this.input.placeholder = 'Search with Nominatim';
    this.input.className = 'geocoder-input';
    
    this.clearButton = document.createElement('button');
    this.clearButton.className = 'geocoder-clear';
    this.clearButton.innerHTML = '×';
    this.clearButton.title = 'Clear search';
    this.clearButton.style.display = 'none';
    
    this.resultsEl = document.createElement('div');
    this.resultsEl.className = 'geocoder-results';
    
    this.input.addEventListener('input', (e) => {
      clearTimeout(this.timeout);
      this.timeout = setTimeout(() => this.geocodeSearch(e.target.value), 500);
      this.clearButton.style.display = e.target.value ? 'block' : 'none';
    });
    
    this.input.addEventListener('keypress', (e) => {
      if (e.key === 'Enter') {
        clearTimeout(this.timeout);
        this.geocodeSearch(e.target.value);
      }
    });
    
    this.clearButton.addEventListener('click', () => {
      this.input.value = '';
      this.resultsEl.innerHTML = '';
      this.resultsEl.style.display = 'none';
      this.clearButton.style.display = 'none';
      if (geocoderMarker) {
        geocoderMarker.remove();
        geocoderMarker = null;
      }
      this.input.focus();
    });
    
    this.inputWrapper.appendChild(this.input);
    this.inputWrapper.appendChild(this.clearButton);
    this.container.appendChild(this.inputWrapper);
    this.container.appendChild(this.resultsEl);
    
    document.addEventListener('click', (e) => {
      if (!this.container.contains(e.target)) this.resultsEl.style.display = 'none';
    });
    
    return this.container;
  }

  onRemove() {
    this.container.parentNode.removeChild(this.container);
    this.map = undefined;
  }
}

export { geocoderMarker };
