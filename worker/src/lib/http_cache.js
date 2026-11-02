import { ifNoneMatchMatches } from './http_etag.js';
import { cachePolicy } from './cache_policy.js';

export const TILE_CACHE_CONTROL = cachePolicy().tileCacheControl;
export const METADATA_CACHE_CONTROL = 'no-cache';

export function cacheRevalidationEnabled(env) {
  return cachePolicy(env).revalidate;
}

// Hash the representation sent to the client, including any image conversion.
export async function conditionalResponse(request, body, { contentType, cacheControl, revalidate = true, legacyMaxAge = 86400000 }) {
  if (!revalidate) {
    return new Response(request.method === 'HEAD' ? null : body, { headers: {
      'Content-Type': contentType,
      'Cache-Control': `max-age=${legacyMaxAge}`,
      'Access-Control-Allow-Origin': '*',
    } });
  }
  const bytes = typeof body === 'string' ? new TextEncoder().encode(body) : body;
  const digest = new Uint8Array(await crypto.subtle.digest('SHA-256', bytes));
  const etag = `"${Array.from(digest, byte => byte.toString(16).padStart(2, '0')).join('')}"`;
  const headers = {
    'Content-Type': contentType,
    'Cache-Control': cacheControl,
    ETag: etag,
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Expose-Headers': 'ETag',
  };
  if (ifNoneMatchMatches(request.headers.get('If-None-Match'), etag)) {
    return new Response(null, { status: 304, headers });
  }
  return new Response(request.method === 'HEAD' ? null : bytes, { headers });
}
