let allDatasets = [];
let selectedCategories = new Set();
let categoryCounts = {};

function toggleNotes(btn) {
    const notesEl = btn.closest('.dataset-item').querySelector('.dataset-notes');
    const visible = notesEl.classList.toggle('visible');
    btn.classList.toggle('active', visible);
    btn.textContent = visible ? 'Notes ▾' : 'Notes ▸';
}
window.toggleNotes = toggleNotes;

async function loadDatasets() {
    const response = await fetch('/api/routes');
    const routes = await response.json();

    allDatasets = Object.entries(routes)
        .filter(([path, info]) => info.type !== 'raster')
        .map(([path, info]) => ({
            path,
            name: info.name,
            type: info.type || 'vector',
            categories: Array.isArray(info.category) ? info.category : (info.category ? [info.category] : []),
            url: info.url || null,
            description: info.description || null,
            notes: info.notes || null
        }))
        .sort((a, b) => a.name.localeCompare(b.name));

    // Calculate category counts once
    categoryCounts = {};
    allDatasets.forEach(dataset => {
        dataset.categories.forEach(cat => {
            categoryCounts[cat] = (categoryCounts[cat] || 0) + 1;
        });
    });

    initializeCategoryFilters();
    loadFromURL();
    filterAndDisplayDatasets();
}

function loadFromURL() {
    const params = new URLSearchParams(window.location.search);

    // Load search term
    const search = params.get('search');
    if (search) {
        document.getElementById('searchInput').value = search;
    }

    // Load categories
    const categories = params.get('categories');
    if (categories) {
        categories.split(',').forEach(cat => {
            selectedCategories.add(cat);
        });

        // Update filter buttons
        document.querySelectorAll('.category-filter').forEach(filter => {
            if (selectedCategories.has(filter.dataset.category)) {
                filter.classList.add('active');
            }
        });

        updateSelectedCategoriesDisplay();
    }
}

function updateURL() {
    const params = new URLSearchParams();

    // Add search term
    const search = document.getElementById('searchInput').value;
    if (search) {
        params.set('search', search);
    }

    // Add categories
    if (selectedCategories.size > 0) {
        params.set('categories', Array.from(selectedCategories).sort().join(','));
    }

    // Update URL without reload
    const newURL = params.toString() ? `${window.location.pathname}?${params.toString()}` : window.location.pathname;
    window.history.replaceState({}, '', newURL);
}

function initializeCategoryFilters() {
    const categories = Object.keys(categoryCounts).sort();
    const filtersEl = document.getElementById('categoryFilters');

    filtersEl.innerHTML = categories.map(category => {
        const count = categoryCounts[category];
        return `<div class="category-filter" data-category="${category}">${category} <span class="category-count">(${count})</span></div>`;
    }).join('');

    filtersEl.querySelectorAll('.category-filter').forEach(filter => {
        filter.addEventListener('click', () => {
            const category = filter.dataset.category;
            toggleCategory(category);
        });
    });
}

function toggleCategory(category) {
    if (selectedCategories.has(category)) {
        selectedCategories.delete(category);
    } else {
        selectedCategories.add(category);
    }

    // Update filter buttons
    document.querySelectorAll('.category-filter').forEach(filter => {
        if (filter.dataset.category === category) {
            filter.classList.toggle('active', selectedCategories.has(category));
        }
    });

    updateSelectedCategoriesDisplay();
    filterAndDisplayDatasets();
}

function updateSelectedCategoriesDisplay() {
    const container = document.getElementById('selectedCategories');
    const sorted = Array.from(selectedCategories).sort();
    container.innerHTML = sorted.map(category =>
        `<div class="selected-category-tag" data-category="${category}">${category} <span class="remove-icon">✕</span></div>`
    ).join('');
    container.querySelectorAll('.selected-category-tag').forEach(tag => {
        tag.addEventListener('click', () => {
            toggleCategory(tag.dataset.category);
        });
    });
}

function filterAndDisplayDatasets() {
    const query = document.getElementById('searchInput').value.toLowerCase();
    let filtered = allDatasets;

    // Filter by selected categories (AND logic - dataset must have ALL selected categories)
    if (selectedCategories.size > 0) {
        filtered = filtered.filter(d => {
            // Check if dataset has ALL selected categories
            for (const selectedCat of selectedCategories) {
                if (!d.categories.includes(selectedCat)) {
                    return false;
                }
            }
            return true;
        });
    }

    // Filter by search query
    if (query) {
        filtered = filtered.filter(d =>
            d.name.toLowerCase().includes(query) ||
            d.path.toLowerCase().includes(query) ||
            d.categories.some(cat => cat.toLowerCase().includes(query)) ||
            (d.description && d.description.toLowerCase().includes(query)) ||
            (d.notes && JSON.stringify(d.notes).toLowerCase().includes(query))
        );
    }

    displayDatasets(filtered);
    updateURL();
}

function displayDatasets(datasets) {
    const listEl = document.getElementById('datasetList');
    const noResultsEl = document.getElementById('noResults');
    const resultsHeader = document.getElementById('resultsHeader');

    // Update results header with counts
    const totalCount = allDatasets.length;
    const filteredCount = datasets.length;
    if (filteredCount === totalCount) {
        resultsHeader.textContent = `Available Sources (${totalCount})`;
    } else {
        resultsHeader.textContent = `Available Sources (${filteredCount} of ${totalCount})`;
    }

    if (datasets.length === 0) {
        listEl.innerHTML = '';
        noResultsEl.style.display = 'block';
        return;
    }

    noResultsEl.style.display = 'none';
    listEl.innerHTML = datasets.map(dataset => {
        // Generate download URL from the dataset URL if it exists
        let downloadLink = '';
        if (dataset.url) {
            const downloadUrl = dataset.url
                .replace('/releases/download/', '/releases/tag/')
                .split('/')
                .slice(0, -1)
                .join('/');
            downloadLink = `<a href="${downloadUrl}" target="_blank">Release Link</a>`;
        }

        return `
            <div class="dataset-item">
                <div class="dataset-name">${dataset.name}</div>
                ${dataset.description ? `<div class="dataset-description">${dataset.description}</div>` : ''}
                ${dataset.categories.length > 0 ? `
                    <div class="dataset-categories">
                        ${dataset.categories.map(cat => `<span class="category-label${selectedCategories.has(cat) ? ' active' : ''}" data-category="${cat}">${cat} <span class="category-count">(${categoryCounts[cat]})</span></span>`).join('')}
                    </div>
                ` : ''}
                <div class="dataset-links">
                    <a href="${dataset.path}tiles.json" target="_blank">TileJSON</a>
                    <a href="${dataset.path}view" target="_blank">View Map</a>
                    ${downloadLink}
                    ${dataset.notes ? `<button class="notes-btn" onclick="toggleNotes(this)">Notes ▸</button>` : ''}
                </div>
                ${dataset.notes ? `<div class="dataset-notes">${dataset.notes}</div>` : ''}
            </div>
        `;
    }).join('');

    // Add click handlers to category labels
    listEl.querySelectorAll('.category-label').forEach(label => {
        label.addEventListener('click', (e) => {
            e.stopPropagation();
            const category = label.dataset.category;
            toggleCategory(category);
        });
    });
}

document.getElementById('searchInput').addEventListener('input', filterAndDisplayDatasets);

// Add collapse/expand functionality for filter section
document.getElementById('filterTitle').addEventListener('click', () => {
    const filterSection = document.getElementById('filterSection');
    filterSection.classList.toggle('collapsed');
});

loadDatasets();
