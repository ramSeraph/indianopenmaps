export class RoutesHandler {
  constructor() {
    this.routesCache = null;
  }

  fetchRoutes() {

    // this should return a promise that resolves when fetching is done

    let promise = new Promise((resolve, reject) => {
      if (this.routesCache !== null) {
        resolve();
        return;
      }

      // Start a new fetch
      fetch('/api/routes')
        .then(response => {
          if (!response.ok) {
            throw new Error(`Failed to fetch routes: ${response.status}`);
          }
          return response.json();
        })
        .then(data => {
          this.routesCache = data;
          resolve();
        })
        .catch(error => {
          console.error('Error fetching routes:', error);
          reject(error);
        });
    });

    return promise;
  }

  getRasterSources() {
    if (this.routesCache === null) {
      throw new Error('Routes not yet fetched. Call fetchRoutes() first.');
    }
    const routes = this.routesCache;
    const rasterRoutes = {};
    for (const [path, info] of Object.entries(routes)) {
      if (info.type === 'raster') {
        rasterRoutes[path] = info;
      }
    }
    return rasterRoutes;
  }

  getVectorSources() {
    if (this.routesCache === null) {
      throw new Error('Routes not yet fetched. Call fetchRoutes() first.');
    }
    const routes = this.routesCache;

    const vectorRoutes = {};
    for (const [path, info] of Object.entries(routes)) {
      if (!info.type || info.type === 'vector') {
        vectorRoutes[path] = info;
      }
    }
    return vectorRoutes;
  }
}
