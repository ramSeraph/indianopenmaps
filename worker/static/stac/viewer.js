// STAC Viewer JavaScript
import { fetchStacPages } from './pagination.js';
import { coversPoint, sheetName } from './sheet-picker.js';
import { tileCacheUrl } from '../shared/tile_cache.js';
const STAC_API_BASE = window.location.origin + '/stac';
const COG_TILER_BASE = window.location.origin + '/cog-tiles';

let map;
let footprints;
const shown = new Map(); // In draw order, keyed by collection and item ID.
const popupChecks = new Map();
let collections = [];
let collectionRequest = null;

// Initialize the map
function initMap() {
  map = L.map('map').setView([20.5937, 78.9629], 5); // Center of India

  const pane = map.createPane('sheetImagery');
  pane.style.zIndex = '410'; // Keep the rasters above the footprint fills.
  pane.style.pointerEvents = 'none';
  document.getElementById('blend').addEventListener('input', updateBlend);
  map.on('click', event => pickAt(event.latlng));
  map.on('popupclose', () => popupChecks.clear());
  const container = map.getContainer();
  new ResizeObserver(() => map.invalidateSize({ pan: false })).observe(container);
  container.setAttribute('aria-label', 'Sheet map. Use the arrow keys to pan and Enter to pick sheets at the centre.');
  container.addEventListener('keydown', event => {
    if (event.key === 'Enter' && event.target === container) {
      event.preventDefault();
      pickAt(map.getCenter(), true);
    }
  });

  // Add ESRI World Imagery as default base layer
  L.tileLayer('https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}', {
    attribution:
      'Tiles &copy; Esri &mdash; Source: Esri, i-cubed, USDA, USGS, AEX, GeoEye, Getmapping, Aerogrid, IGN, IGP, UPR-EGP, and the GIS User Community',
    maxZoom: 19
  }).addTo(map);
}

// Load collections from STAC API
async function loadCollections() {
  try {
    collections = await fetchStacPages(`${STAC_API_BASE}/collections`, 'collections');

    renderCollections();
    const id = new URL(location.href).searchParams.get('collection');
    if (collections.some(collection => collection.id === id)) loadCollection(id);
  } catch (error) {
    console.error('Error loading collections:', error);
    document
      .getElementById('collections-list')
      .replaceChildren(element('p', 'error-message', 'Failed to load collections: ' + error.message));
    document.getElementById('collection-select').options[0].textContent = 'Could not load collections';
  }
}

// Render collections in sidebar
function renderCollections() {
  const listContainer = document.getElementById('collections-list');

  const select = document.getElementById('collection-select');
  select.options[0].textContent = collections.length ? 'Choose a collection' : 'No collections available';
  if (collections.length === 0) {
    listContainer.innerHTML = '<p style="padding: 20px; color: #999;">No collections available</p>';
    return;
  }

  listContainer.replaceChildren();
  for (const collection of collections) {
    const button = element('button', 'collection-item');
    button.type = 'button';
    button.dataset.id = collection.id;
    button.setAttribute('aria-pressed', 'false');
    button.append(
      element('span', 'collection-title', collection.title || collection.id),
      element('span', 'collection-id', collection.id)
    );
    button.addEventListener('click', () => loadCollection(collection.id));
    listContainer.append(button);
    select.add(new Option(collection.title || collection.id, collection.id));
  }
  select.addEventListener('change', () => loadCollection(select.value));
}

// Load a specific collection and its items
async function loadCollection(collectionId) {
  collectionRequest?.abort();
  const controller = new AbortController();
  collectionRequest = controller;
  // Update UI
  document.querySelectorAll('.collection-item').forEach(item => {
    const active = item.dataset.id === collectionId;
    item.classList.toggle('active', active);
    item.setAttribute('aria-pressed', String(active));
  });

  document.getElementById('collection-select').value = collectionId;
  const url = new URL(location.href);
  url.searchParams.set('collection', collectionId);
  history.replaceState(null, '', url);
  document.getElementById('pick-hint').textContent = '';

  showLoading('Loading items...');

  try {
    // Clear existing layers
    map.closePopup();
    footprints?.remove();
    footprints = null;
    document.getElementById('info-panel').classList.add('hidden');

    // Load collection details
    const collectionResponse = await fetch(`${STAC_API_BASE}/collections/${collectionId}`, {
      signal: controller.signal
    });
    if (!collectionResponse.ok) throw new Error(`HTTP ${collectionResponse.status}`);
    const collection = await collectionResponse.json();

    // Load all items
    const features = await fetchStacPages(`${STAC_API_BASE}/collections/${collectionId}/items?limit=1000`, 'features', {
      signal: controller.signal,
      onProgress: (count, total) => showLoading(`Loading items: ${count}${total === undefined ? '' : ` / ${total}`}...`)
    });

    if (controller.signal.aborted) return;
    if (features.length > 0) {
      renderItems(features);
      showInfo(collection, features.length);
      document.getElementById('pick-hint').textContent = 'Click a footprint to choose sheets.';
    } else {
      alert('No items found in this collection');
    }
  } catch (error) {
    if (error.name === 'AbortError') return;
    console.error('Error loading collection:', error);
    alert(`Failed to load collection: ${error.message}`);
  } finally {
    if (collectionRequest === controller) hideLoading();
  }
}

function element(tag, className, text) {
  const node = document.createElement(tag);
  if (className) node.className = className;
  if (text != null) node.textContent = text;
  return node;
}

function renderItems(features) {
  footprints = L.geoJSON(features, {
    pointToLayer: (_, latlng) => L.circleMarker(latlng, { radius: 6 }),
    style: { color: '#4a9eff', weight: 1.5, opacity: 0.85, fillOpacity: 0.13 }
  }).addTo(map);
  const bounds = footprints.getBounds();
  if (bounds.isValid() && (!shown.size || !bounds.intersects(map.getBounds()))) {
    map.fitBounds(bounds, { padding: [24, 24], maxZoom: 15 });
  }
}

const itemKey = item => JSON.stringify([item.collection, item.id]);
const cogHref = item => item.assets?.cog?.href || item.assets?.data?.href;
const collator = new Intl.Collator(undefined, { numeric: true });

function pickAt(latlng, keyboard = false) {
  if (!footprints) return;
  const point = map.latLngToContainerPoint(latlng);
  const rect = map.getContainer().getBoundingClientRect();
  const elements = new Set(document.elementsFromPoint(rect.left + point.x, rect.top + point.y));
  const matches = footprints
    .getLayers()
    .filter(layer => coversPoint(layer, latlng, elements))
    .map(layer => layer.feature);
  map.closePopup();
  document.getElementById('pick-hint').textContent = matches.length ? '' : 'No sheets here in this collection.';
  if (!matches.length) return;

  matches.sort((a, b) => collator.compare(sheetName(a), sheetName(b)) || collator.compare(a.id, b.id));
  const panel = element('section', 'sheet-picker');
  const title = matches.length === 1 ? '1 sheet here' : matches.length + ' sheets here';
  panel.setAttribute('aria-label', title);
  panel.append(element('h2', '', title));
  const list = element('div', 'picker-list');
  panel.append(list);
  for (const item of matches) {
    const key = itemKey(item);
    const row = element('div', 'picker-row');
    const label = element('label', 'picker-choice');
    const box = element('input');
    box.type = 'checkbox';
    box.checked = shown.has(key);
    box.disabled = !cogHref(item);
    box.addEventListener('change', () => setShown(item, box.checked));
    popupChecks.set(key, box);
    const name = element('span', '', sheetName(item));
    if (item.properties?.title && item.properties.title !== item.id)
      name.append(element('small', 'picker-id', item.id));
    label.append(box, name);
    row.append(label);
    if (box.disabled) row.append(element('span', 'preview-unavailable', 'Map preview unavailable'));
    row.append(itemDetails(item));
    list.append(row);
  }
  const width = Math.min(340, Math.max(130, map.getSize().x - 60));
  panel.style.width = width + 'px';
  const popup = L.popup({ maxWidth: width, className: 'sheet-popup', autoPanPadding: [10, 10] })
    .setLatLng(latlng)
    .setContent(panel)
    .openOn(map);
  panel.addEventListener(
    'toggle',
    () => {
      if (!popup.isOpen()) return;
      const focused = document.activeElement;
      popup.update();
      if (panel.contains(focused)) focused.focus({ preventScroll: true });
    },
    true
  );
  panel.addEventListener('keydown', event => {
    if (event.key === 'Escape') {
      event.stopPropagation();
      map.closePopup();
      map.getContainer().focus({ preventScroll: true });
    }
  });
  if (keyboard)
    (panel.querySelector('input:not(:disabled)') || panel.querySelector('summary'))?.focus({ preventScroll: true });
}

function itemDetails(item) {
  const details = element('details', 'picker-details');
  details.append(element('summary', '', 'Files & details'));
  const files = element('div', 'popup-assets');
  for (const [key, asset] of Object.entries(item.assets || {})) {
    if (!asset.href) continue;
    const link = element('a', 'asset-link', asset.title || { cog: 'GeoTIFF', original: 'Original scan' }[key] || key);
    if (!URL.canParse(asset.href, location.href)) continue;
    const url = new URL(asset.href, location.href);
    if (!['http:', 'https:'].includes(url.protocol)) continue;
    link.href = url.href;
    link.target = '_blank';
    link.rel = 'noopener';
    files.append(link);
  }
  const metadata = element('dl', 'item-metadata');
  for (const [key, value] of Object.entries({ ID: item.id, ...item.properties })) {
    if (key.startsWith('proj:') || value == null) continue;
    const fields =
      key === 'iomaps:source_properties' && typeof value === 'object' ? Object.entries(value) : [[key, value]];
    for (const [name, data] of fields) {
      if (data != null)
        metadata.append(
          element('dt', '', name),
          element('dd', '', typeof data === 'object' ? JSON.stringify(data) : data)
        );
    }
  }
  details.append(files, metadata);
  return details;
}

function setShown(item, visible) {
  const key = itemKey(item);
  if (visible) {
    const tileUrl = tileCacheUrl(COG_TILER_BASE + '/{z}/{x}/{y}?url=' + encodeURIComponent(cogHref(item)));
    const bbox = item.bbox;
    const end = bbox?.length / 2;
    const bounds =
      bbox && bbox[0] <= bbox[end]
        ? [
            [bbox[1], bbox[0]],
            [bbox[end + 1], bbox[end]]
          ]
        : undefined;
    const layer = L.tileLayer(tileUrl, { pane: 'sheetImagery', bounds, maxZoom: 20, attribution: 'COG Tiles' })
      .addTo(map)
      .bringToFront();
    shown.set(key, { item, layer });
  } else {
    shown.get(key)?.layer.remove();
    shown.delete(key);
  }
  const checkbox = popupChecks.get(key);
  if (checkbox) checkbox.checked = visible;
  renderShown();
  if (visible) document.getElementById('shown-list').scrollTop = 0;
}

function updateBlend() {
  const control = document.getElementById('blend-control');
  const slider = document.getElementById('blend');
  const entries = [...shown.values()];
  control.hidden = entries.length < 2;
  if (control.hidden) slider.value = 100;
  const opacity = Number(slider.value);
  control.querySelector('output').value = opacity + '%';
  for (const entry of entries) entry.layer.setOpacity(entry === entries.at(-1) ? opacity / 100 : 1);
}

function renderShown(focusKey) {
  const section = document.getElementById('shown-panel');
  const list = document.getElementById('shown-list');
  list.replaceChildren();
  section.hidden = !shown.size;
  document.getElementById('shown-heading').textContent = 'On the map (' + shown.size + ')';
  let focusTarget;
  for (const [key, entry] of [...shown].reverse()) {
    const name = sheetName(entry.item);
    const row = element('li', 'shown-row');
    const heading = element('div', 'shown-name', name);
    const controls = element('div', 'shown-actions');
    const isTop = !list.childElementCount;
    if (isTop) controls.append(element('span', 'top-label', 'Top layer'));
    else {
      const front = element('button', '', 'Bring to top');
      front.setAttribute('aria-label', 'Bring ' + name + ' to top');
      front.addEventListener('click', () => {
        entry.layer.bringToFront();
        shown.delete(key);
        shown.set(key, entry);
        renderShown(key);
      });
      controls.append(front);
    }
    const remove = element('button', '', 'Remove');
    remove.setAttribute('aria-label', 'Remove ' + name);
    remove.addEventListener('click', () => {
      const adjacent = row.nextElementSibling || row.previousElementSibling;
      const nextKey = adjacent?.dataset.key;
      setShown(entry.item, false);
      (
        Array.from(list.children)
          .find(child => child.dataset.key === nextKey)
          ?.querySelector('button') || map.getContainer()
      ).focus({ preventScroll: true });
    });
    if (key === focusKey) {
      heading.tabIndex = -1;
      focusTarget = heading;
    }
    row.dataset.key = key;
    const collection = collections.find(c => c.id === entry.item.collection);
    row.append(heading, element('span', 'shown-collection', collection?.title || entry.item.collection), controls);
    controls.append(remove);
    list.append(row);
  }
  updateBlend();
  if (focusTarget) {
    list.scrollTop = 0;
    focusTarget.focus({ preventScroll: true });
  }
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

document.addEventListener('DOMContentLoaded', () => {
  initMap();
  loadCollections();
});
