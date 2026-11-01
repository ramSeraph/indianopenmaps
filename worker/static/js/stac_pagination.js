export async function fetchStacPages(url, key, { signal, onProgress = () => {} } = {}) {
    const values = [];
    const visited = new Set();
    while (url) {
        if (visited.has(url)) throw new Error('The STAC API returned a repeated page');
        visited.add(url);
        const response = await fetch(url, { signal });
        if (!response.ok) throw new Error(`STAC request failed: HTTP ${response.status}`);
        const page = await response.json();
        values.push(...(page[key] || []));
        onProgress(values.length, page.numberMatched);
        const next = page.links?.find(link => link.rel === 'next');
        url = next ? new URL(next.href, response.url || url).href : null;
    }
    return values;
}
