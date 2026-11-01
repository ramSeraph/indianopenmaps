// Local workerd server for independent STAC clients, backed by real Parquet files.
// Usage: node test/helpers/stac_server.mjs collection-id=/absolute/index.parquet [...]
import { readFile } from 'node:fs/promises';
import { resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { build } from 'esbuild';
import { Miniflare } from 'miniflare';

const root = fileURLToPath(new URL('../../', import.meta.url));
const files = new Map();
const catalog = { id: 'test', title: 'STAC integration fixtures', description: 'Real release indexes in workerd', links: [] };
for (const arg of process.argv.slice(2)) {
  const split = arg.indexOf('=');
  if (split < 1) throw new Error('Expected collection-id=/absolute/index.parquet');
  const id = arg.slice(0, split), path = resolve(arg.slice(split + 1));
  const url = `https://fixtures.example/${id}.parquet`;
  files.set(url, await readFile(path));
  let metadata;
  try { metadata = JSON.parse(await readFile(path.replace(/\.parquet$/, '.collection.json'), 'utf8')); }
  catch (error) { if (error.code !== 'ENOENT') throw error; }
  catalog.links.push({ rel: 'child', href: `./${id}`, title: metadata?.title || id, geoparquet: url, collection: metadata });
}
if (!files.size) throw new Error('Supply at least one index');
const bundle = await build({
  stdin: { resolveDir: root, contents: `import { Hono } from 'hono';
    import { registerStacRoutes } from './src/routes/stac.js';
    const app = new Hono(); await registerStacRoutes(app, console, ${JSON.stringify(catalog)}); export default app;` },
  bundle: true, format: 'esm', platform: 'browser', conditions: ['workerd'], target: 'es2022', write: false,
  external: ['node:*'],
  alias: { module: 'node:module' },
});
const worker = new Miniflare({
  port: Number(process.env.STAC_TEST_PORT || 8791),
  compatibilityDate: '2024-11-01', compatibilityFlags: ['nodejs_compat', 'no_handle_cross_request_promise_resolution'],
  modules: true, script: bundle.outputFiles[0].text,
  outboundService: request => {
    const bytes = files.get(request.url);
    if (!bytes) return new Response('Fixture not found', { status: 404 });
    if (request.method === 'HEAD') return new Response(null, { headers: { 'Content-Length': String(bytes.length) } });
    const range = request.headers.get('range')?.match(/^bytes=(\d+)-(\d+)$/);
    if (!range) return new Response('Range required', { status: 400 });
    const [, start, end] = range.map(Number);
    return new Response(bytes.subarray(start, end + 1), { status: 206, headers: {
      'Content-Type': 'application/vnd.apache.parquet', 'Content-Range': `bytes ${start}-${end}/${bytes.length}`,
    } });
  },
});
console.log(`STAC ready: ${await worker.ready}stac`);
for (const signal of ['SIGINT', 'SIGTERM']) process.on(signal, async () => { await worker.dispose(); process.exit(0); });
