/* banditx service worker */
const CACHE = 'banditx-v1';
const PRECACHE = ['/', '/en', '/manifest.webmanifest', '/icon-192.png', '/icon-512.png'];

self.addEventListener('install', (event) => {
  event.waitUntil(
    caches.open(CACHE).then((c) => c.addAll(PRECACHE)).then(() => self.skipWaiting())
  );
});

self.addEventListener('activate', (event) => {
  event.waitUntil(
    caches.keys().then((keys) =>
      Promise.all(keys.filter((k) => k !== CACHE).map((k) => caches.delete(k)))
    ).then(() => self.clients.claim())
  );
});

self.addEventListener('fetch', (event) => {
  const req = event.request;
  if (req.method !== 'GET') return;

  const url = new URL(req.url);
  if (url.origin !== self.location.origin) return;

  // Не кэшируем RSC-запросы и страницы навигации — пусть идут в сеть
  if (url.searchParams.has('_rsc')) return;

  // Cache-first для статики
  if (
    url.pathname.startsWith('/_next/static/') ||
    /\.(?:woff2?|png|jpe?g|gif|webp|svg|ico|css|js)$/i.test(url.pathname)
  ) {
    event.respondWith(
      caches.match(req).then((hit) =>
        hit || fetch(req).then((res) => {
          const copy = res.clone();
          caches.open(CACHE).then((c) => c.put(req, copy));
          return res;
        })
      )
    );
    return;
  }

  // Network-first для остального
  event.respondWith(
    fetch(req).catch(() => caches.match(req).then((hit) => hit || caches.match('/')))
  );
});