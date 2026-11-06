let allDatasets = [];

        async function loadDatasets() {
            const response = await fetch('/api/routes');
            const routes = await response.json();

            allDatasets = Object.entries(routes)
                .filter(([path, info]) => info.type === 'raster')
                .map(([path, info]) => ({
                    path,
                    name: info.name,
                    url: info.url || null
                }))
;

            displayDatasets(allDatasets);
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
                    downloadLink = `<a href="${downloadUrl}" target="_blank">Download</a>`;
                }

                return `
                    <div class="dataset-item">
                        <div class="dataset-name">${dataset.name}</div>
                        <div class="dataset-links">
                            <a href="${dataset.path}tiles.json" target="_blank">TileJSON</a>
                            <a href="${dataset.path}rasterview" target="_blank">View Map</a>
                            ${downloadLink}
                        </div>
                    </div>
                `;
            }).join('');
        }

        document.getElementById('searchInput').addEventListener('input', (e) => {
            const query = e.target.value.toLowerCase();
            const filtered = allDatasets.filter(d =>
                d.name.toLowerCase().includes(query) ||
                d.path.toLowerCase().includes(query)
            );
            displayDatasets(filtered);
        });

        loadDatasets();
