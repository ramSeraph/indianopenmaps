// STAC Viewer JavaScript
const STAC_API_BASE = window.location.origin + '/stac';

let map;
let currentLayers = [];
let collections = [];
let selectedCollection = null;

// Initialize the map
function initMap() {
    map = L.map('map').setView([20.5937, 78.9629], 5); // Center of India
    
    // Add dark tile layer
    L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
        attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors &copy; <a href="https://carto.com/attributions">CARTO</a>',
        subdomains: 'abcd',
        maxZoom: 20
    }).addTo(map);
}

// Load collections from STAC API
async function loadCollections() {
    try {
        const response = await fetch(`${STAC_API_BASE}/collections`);
        if (!response.ok) {
            throw new Error(`Failed to load collections: ${response.statusText}`);
        }
        
        const data = await response.json();
        collections = data.collections || [];
        
        renderCollections();
    } catch (error) {
        console.error('Error loading collections:', error);
        document.getElementById('collections-list').innerHTML = `
            <div class="error-message">
                Failed to load collections: ${error.message}
            </div>
        `;
    }
}

// Render collections in sidebar
function renderCollections() {
    const listContainer = document.getElementById('collections-list');
    
    if (collections.length === 0) {
        listContainer.innerHTML = '<p style="padding: 20px; color: #999;">No collections available</p>';
        return;
    }
    
    listContainer.innerHTML = collections.map(collection => `
        <div class="collection-item" data-id="${collection.id}">
            <div class="collection-title">${collection.title || collection.id}</div>
            <div class="collection-id">${collection.id}</div>
            <div class="collection-stats">
                <div class="stat">
                    <span>📊</span>
                    <span>STAC v${collection.stac_version || '1.0.0'}</span>
                </div>
            </div>
        </div>
    `).join('');
    
    // Add click handlers
    document.querySelectorAll('.collection-item').forEach(item => {
        item.addEventListener('click', () => {
            const collectionId = item.dataset.id;
            loadCollection(collectionId);
        });
    });
}

// Load a specific collection and its items
async function loadCollection(collectionId) {
    // Update UI
    document.querySelectorAll('.collection-item').forEach(item => {
        item.classList.toggle('active', item.dataset.id === collectionId);
    });
    
    selectedCollection = collections.find(c => c.id === collectionId);
    
    showLoading('Loading items...');
    
    try {
        // Clear existing layers
        clearLayers();
        
        // Load collection details
        const collectionResponse = await fetch(`${STAC_API_BASE}/collections/${collectionId}`);
        const collection = await collectionResponse.json();
        
        // Load items (limiting to 100 for performance)
        const itemsResponse = await fetch(`${STAC_API_BASE}/collections/${collectionId}/items?limit=100`);
        const itemsData = await itemsResponse.json();
        
        console.log('Loaded items:', itemsData);
        
        if (itemsData.features && itemsData.features.length > 0) {
            renderItems(itemsData.features, collection);
            showInfo(collection, itemsData.features.length);
        } else {
            alert('No items found in this collection');
        }
        
    } catch (error) {
        console.error('Error loading collection:', error);
        alert(`Failed to load collection: ${error.message}`);
    } finally {
        hideLoading();
    }
}

// Render items on the map
function renderItems(features, collection) {
    // Create a GeoJSON layer
    const geojsonLayer = L.geoJSON(features, {
        pointToLayer: (feature, latlng) => {
            return L.circleMarker(latlng, {
                radius: 6,
                fillColor: '#4a9eff',
                color: '#fff',
                weight: 1,
                opacity: 1,
                fillOpacity: 0.7
            });
        },
        style: (feature) => {
            return {
                color: '#4a9eff',
                weight: 2,
                opacity: 0.8,
                fillOpacity: 0.3
            };
        },
        onEachFeature: (feature, layer) => {
            // Create popup content
            let popupContent = `<div class="popup-title">${feature.id}</div>`;
            
            if (feature.properties) {
                popupContent += '<div class="popup-item">';
                const props = feature.properties;
                Object.keys(props).slice(0, 5).forEach(key => {
                    popupContent += `<div><span class="popup-label">${key}:</span> ${props[key]}</div>`;
                });
                popupContent += '</div>';
            }
            
            if (feature.assets && Object.keys(feature.assets).length > 0) {
                popupContent += '<div class="popup-assets">';
                popupContent += '<div class="popup-label">Assets:</div>';
                Object.entries(feature.assets).forEach(([key, asset]) => {
                    if (asset.href) {
                        popupContent += `<a href="${asset.href}" target="_blank" class="asset-link">${key}</a>`;
                    } else {
                        popupContent += `<span class="asset-link" style="opacity: 0.5;">${key}</span>`;
                    }
                });
                popupContent += '</div>';
            }
            
            layer.bindPopup(popupContent);
        }
    });
    
    geojsonLayer.addTo(map);
    currentLayers.push(geojsonLayer);
    
    // Fit bounds to show all features
    try {
        const bounds = geojsonLayer.getBounds();
        if (bounds.isValid()) {
            map.fitBounds(bounds, { padding: [50, 50], maxZoom: 15 });
        }
    } catch (e) {
        console.warn('Could not fit bounds:', e);
    }
}

// Clear all layers from map
function clearLayers() {
    currentLayers.forEach(layer => {
        map.removeLayer(layer);
    });
    currentLayers = [];
}

// Show loading indicator
function showLoading(text = 'Loading...') {
    const loadingEl = document.getElementById('map-loading');
    document.getElementById('loading-text').textContent = text;
    loadingEl.classList.remove('hidden');
}

// Hide loading indicator
function hideLoading() {
    document.getElementById('map-loading').classList.add('hidden');
}

// Show info panel
function showInfo(collection, itemCount) {
    const infoPanel = document.getElementById('info-panel');
    const infoContent = document.getElementById('info-content');
    
    let bbox = 'Unknown';
    if (collection.extent && collection.extent.spatial && collection.extent.spatial.bbox) {
        const b = collection.extent.spatial.bbox[0];
        bbox = `[${b[0].toFixed(2)}, ${b[1].toFixed(2)}, ${b[2].toFixed(2)}, ${b[3].toFixed(2)}]`;
    }
    
    infoContent.innerHTML = `
        <div class="info-item">
            <span class="info-label">Collection:</span>
            ${collection.title || collection.id}
        </div>
        <div class="info-item">
            <span class="info-label">Items loaded:</span>
            ${itemCount}
        </div>
        <div class="info-item">
            <span class="info-label">Bbox:</span>
            ${bbox}
        </div>
        <div class="info-item">
            <span class="info-label">License:</span>
            ${collection.license || 'Unknown'}
        </div>
    `;
    
    infoPanel.classList.remove('hidden');
}

// Initialize the application
document.addEventListener('DOMContentLoaded', () => {
    initMap();
    loadCollections();
});
