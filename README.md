\# banditx



Fast reverse proxy in Rust. HTTP/1.1, HTTP/2 over TLS (ALPN), FastCGI.

Pure Rust — no OpenSSL, no C dependencies.



\## Features



\- \*\*HTTP/1.1\*\* — keep-alive, chunked encoding, hop-by-hop stripping

\- \*\*HTTP/2 over TLS\*\* — ALPN negotiation, flow control, streaming

\- \*\*FastCGI\*\* — speaks directly to PHP-FPM, works over both HTTP/1.1 and HTTP/2

\- \*\*Routing\*\* — longest-prefix match, per-route kind (`http` / `fastcgi`)

\- \*\*TLS 1.3\*\* — via rustls + ring, self-signed cert auto-generated

\- \*\*Static files\*\* — ETag, Last-Modified, Range, 304, `safe\_join`

\- \*\*gzip\*\* — on the fly for static and proxied text/JSON/XML responses

\- \*\*Load balancing\*\* — round-robin, least-conn, ip-hash

\- \*\*Health checks\*\* — TCP probe every 3s, dead upstreams excluded

\- \*\*Hot reload\*\* — config changes apply without dropping connections

\- \*\*Metrics\*\* — Prometheus exposition on `/\_banditx/metrics`

\- \*\*Access log\*\* — combined (nginx style) or JSON, async queue



\## Quick start



```bash

cargo run

```



Then:



```bash

curl http://127.0.0.1:8080/

curl -k https://127.0.0.1:8443/

```



\## Config example



```yaml

server:

&#x20; listen: "127.0.0.1:8080"

&#x20; tls\_listen: "127.0.0.1:8443"

&#x20; access\_log:

&#x20;   path: "access.log"

&#x20;   format: json



routes:

&#x20; - prefix: "/static/"

&#x20;   static\_root: "/var/www/static"



&#x20; - prefix: "/php/"

&#x20;   upstreams: \["127.0.0.1:9000"]

&#x20;   kind: fastcgi

&#x20;   doc\_root: "/var/www/html"



&#x20; - prefix: "/api/"

&#x20;   upstreams: \["127.0.0.1:8000", "127.0.0.1:8001"]

&#x20;   strategy: least\_conn



&#x20; - prefix: "/"

&#x20;   upstreams: \["127.0.0.1:3000"]

```



\## Benchmark vs nginx



\*\*Setup:\*\* Windows 11, 4 cores, single host for client and server.

\*\*Tool:\*\* \[`oha 1.16.0`](https://github.com/hatoo/oha).

\*\*Payload:\*\* `/static/hello.txt` — 31 bytes, served from disk.

\*\*Load:\*\* 10 000 requests, 50 concurrent connections.



| Server | Requests/sec | p50 latency | p99 latency |

|--------|--------------|-------------|-------------|

| \*\*banditx\*\* v0.3.3 | \*\*5019\*\* | 9.3 ms | 22.4 ms |

| nginx 1.27 (Windows build) | 2819 | 17.4 ms | 21.4 ms |



banditx is \~1.8× faster on this benchmark.



\*\*Important caveat:\*\* this is \*not\* because banditx is a better proxy.

nginx was written for Linux and uses `select()` emulation on Windows.

banditx uses tokio, which takes advantage of Windows-native IOCP. On

Linux the comparison is very likely reversed — nginx's `epoll` +

`sendfile` path is significantly more optimized than what banditx can

do today.



We publish this number only to document where we are, not to claim

superiority. The honest conclusion: \*\*on Windows, banditx is a viable

choice because nginx is not Windows-native.\*\*



\## Status



\- v0.0.1 — HTTP server, single response

\- v0.0.2 — HTTP/1.1 reverse proxy

\- v0.0.3 — YAML config, routing, XFF

\- v0.0.4 — TLS 1.3 via rustls

\- v0.0.5 — HTTP/2 over ALPN

\- v0.0.6 — FastCGI

\- v0.1.0 — FastCGI pool, hot-reload, metrics, access log, graceful shutdown

\- v0.2.0 — balancer, health checks

\- v0.3.0 — static files with ETag/Range

\- v0.3.1 — gzip for static

\- v0.3.2 — JSON access log via async queue

\- v0.3.3 — FastCGI over HTTP/2

\- v0.3.4 — gzip for FastCGI-proxied responses ← current



\## Not yet done



\- Multi-worker (SO\_REUSEPORT)

\- DNS-based dynamic upstreams

\- Disk cache

\- HTTP/3

\- `.banditx` per-directory config files

\- Graceful shutdown on SIGTERM



\## License



MIT

