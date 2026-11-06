import { setWorkerUrl } from 'https://esm.sh/maplibre-gl@6.16.0';

// MapLibre 6's relative worker URL does not resolve through esm.sh. Keep its
// entry point same-origin and load the matching, self-contained worker module.
setWorkerUrl(new URL('./maplibre_worker.mjs', import.meta.url).href);

// Shared by the map controls and viewers so every component uses one runtime.
export * from 'https://esm.sh/maplibre-gl@6.16.0';
