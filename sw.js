/* banditx service worker */
const CACHE = 'banditx-v2'; // ← версию подняли, чтобы старая битая чистка снеслась

const PRECACHE = [
  '/',
  '/en',
  '/manifest.webmanifest',
  '/icons/android-chrome-192x192.png',
  '/icons/android-chrome-512x512.png',
  '/icons/favicon.svg',
];

self.addEventListener('install', (event) => {
  event.waitUntil(
    (async () => {
      const cache = await caches.open(CACHE);
      // Толерантная предзагрузка: один 404 не убивает install.
      await Promise.all(
        PRECACHE.map(async (url) => {
          try {
            const res = await fetch(url, { cache: 'no-cache' });
            if (res.ok) await cache.put(url, res.clone());
            else console.warn('[sw] precache skip', url, res.status);
          } catch (e) {
            console.warn('[sw] precache fail', url, e);
          }
        })
      );
      await self.skipWaiting();
    })()
  );
});

self.addEventListener('activate', (event) => {
  event.waitUntil(
    caches.keys()
      .then((keys) =>
        Promise.all(keys.filter((k) => k !== CACHE).map((k) => caches.delete(k)))
      )
      .then(() => self.clients.claim())
  );
});

self.addEventListener('fetch', (event) => {
  const req = event.request;
  if (req.method !== 'GET') return;

  const url = new URL(req.url);
  if (url.origin !== self.location.origin) return;

  // RSC-запросы и навигацию — всегда в сеть
  if (url.searchParams.has('_rsc')) return;

  // Cache-first для статики
  if (
    url.pathname.startsWith('/_next/static/') ||
    /\.(?:woff2?|png|jpe?g|gif|webp|svg|ico|css|js)$/i.test(url.pathname)
  ) {
    event.respondWith(
      caches.match(req).then((hit) =>
        hit ||
        fetch(req)
          .then((res) => {
            if (res.ok) {
              const copy = res.clone();
              caches.open(CACHE).then((c) => c.put(req, copy));
            }
            return res;
          })
          .catch(() => hit) // на всякий
      )
    );
    return;
  }

  // Network-first для остального
  event.respondWith(
    fetch(req).catch(() =>
      caches.match(req).then((hit) => hit || caches.match('/'))
    )
  );
});