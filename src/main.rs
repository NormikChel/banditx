//! banditx v0.3.2 — static + gzip + JSON access log + async-очередь логов
//!
//! Изменения от v0.2.0:
//!  - Статика: ETag, Last-Modified, Range, 304, safe_join
//!  - gzip на лету для text/*, JSON, JS, XML, SVG
//!  - Access log через mpsc-очередь, не блокирует request-путь
//!  - Форматы: combined (nginx) и json
//!  - h2-запросы теперь тоже логируются
//!  - duration_ms в логах

use std::collections::HashMap;
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;
use bytes::Bytes;
use h2::server as h2_server;
use http::{HeaderValue, Response};
use serde::Deserialize;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, Notify};
use tokio::time::timeout;
use tokio_rustls::TlsAcceptor;

// ============================================================
//  КОНСТАНТЫ
// ============================================================

const MAX_HEADER_BYTES: usize = 64 * 1024;
const HEADER_TIMEOUT: Duration = Duration::from_secs(15);
const BODY_TIMEOUT: Duration = Duration::from_secs(120);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_FCGI_BODY: usize = 16 * 1024 * 1024;
const FCGI_IDLE_MAX: usize = 64;
const HEALTH_INTERVAL: Duration = Duration::from_secs(3);
const HEALTH_TIMEOUT: Duration = Duration::from_secs(1);
const HEALTH_FAIL_THRESHOLD: u32 = 2;
const GZIP_MIN: usize = 512;
const GZIP_MAX: usize = 4 * 1024 * 1024;

const FCGI_VERSION_1: u8 = 1;
const FCGI_BEGIN_REQUEST: u8 = 1;
const FCGI_END_REQUEST: u8 = 3;
const FCGI_PARAMS: u8 = 4;
const FCGI_STDIN: u8 = 5;
const FCGI_STDOUT: u8 = 6;
const FCGI_STDERR: u8 = 7;
const FCGI_RESPONDER: u16 = 1;

// ============================================================
//  КОНФИГ
// ============================================================

#[derive(Debug, Deserialize, Clone)]
struct Config {
    server: ServerCfg,
    routes: Vec<RouteCfg>,
}

#[derive(Debug, Deserialize, Clone)]
struct ServerCfg {
    listen: String,
    #[serde(default)] tls_listen: Option<String>,
    #[serde(default)] access_log: Option<AccessLogCfg>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
enum AccessLogCfg {
    Path(String),
    Full {
        #[serde(default)] path: Option<String>,
        #[serde(default)] format: LogFormat,
    },
}

#[derive(Debug, Clone, Copy, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
enum LogFormat {
    #[default]
    Combined,
    Json,
}

#[derive(Debug, Deserialize, Clone)]
struct RouteCfg {
    prefix: String,
    #[serde(default)] upstreams: Vec<String>,
    #[serde(default)] strategy: Strategy,
    #[serde(default)] kind: RouteKind,
    #[serde(default)] doc_root: Option<String>,
    #[serde(default)] static_root: Option<String>,
}

#[derive(Debug, Clone, Copy, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum Strategy {
    #[default] RoundRobin,
    LeastConn,
    IpHash,
}

#[derive(Debug, Clone, Copy, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
enum RouteKind {
    #[default] Http,
    Fastcgi,
}

impl Config {
    fn pick(&self, path: &str) -> Option<&RouteCfg> {
        self.routes.iter()
            .filter(|r| path.starts_with(&r.prefix))
            .max_by_key(|r| r.prefix.len())
    }
}

fn load_config(path: &str) -> io::Result<Config> {
    let text = std::fs::read_to_string(path)?;
    let cfg: Config = serde_yaml::from_str(&text)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("config: {e}")))?;
    if cfg.routes.is_empty() {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "no routes"));
    }
    Ok(cfg)
}

// ============================================================
//  МЕТРИКИ
// ============================================================

struct Metrics {
    requests_total: AtomicU64,
    requests_2xx: AtomicU64,
    requests_4xx: AtomicU64,
    requests_5xx: AtomicU64,
    bytes_in: AtomicU64,
    bytes_out: AtomicU64,
    active_conns: AtomicUsize,
    fcgi_pool_hits: AtomicU64,
    fcgi_pool_misses: AtomicU64,
    upstream_errors: AtomicU64,
    health_check_failures: AtomicU64,
}

impl Metrics {
    fn new() -> Self {
        Self {
            requests_total: AtomicU64::new(0),
            requests_2xx: AtomicU64::new(0),
            requests_4xx: AtomicU64::new(0),
            requests_5xx: AtomicU64::new(0),
            bytes_in: AtomicU64::new(0),
            bytes_out: AtomicU64::new(0),
            active_conns: AtomicUsize::new(0),
            fcgi_pool_hits: AtomicU64::new(0),
            fcgi_pool_misses: AtomicU64::new(0),
            upstream_errors: AtomicU64::new(0),
            health_check_failures: AtomicU64::new(0),
        }
    }

    fn record_status(&self, code: u16) {
        self.requests_total.fetch_add(1, Ordering::Relaxed);
        match code / 100 {
            2 => { self.requests_2xx.fetch_add(1, Ordering::Relaxed); }
            4 => { self.requests_4xx.fetch_add(1, Ordering::Relaxed); }
            5 => { self.requests_5xx.fetch_add(1, Ordering::Relaxed); }
            _ => {}
        }
    }

    fn render(&self, routes: &[RouteCfg], balancers: &HashMap<String, Arc<Balancer>>) -> String {
        let mut s = String::with_capacity(2048);
        let g = |s: &mut String, name: &str, help: &str, v: u64| {
            s.push_str(&format!("# HELP {name} {help}\n# TYPE {name} counter\n{name} {v}\n"));
        };
        g(&mut s, "banditx_requests_total", "Total requests", self.requests_total.load(Ordering::Relaxed));
        g(&mut s, "banditx_requests_2xx", "2xx", self.requests_2xx.load(Ordering::Relaxed));
        g(&mut s, "banditx_requests_4xx", "4xx", self.requests_4xx.load(Ordering::Relaxed));
        g(&mut s, "banditx_requests_5xx", "5xx", self.requests_5xx.load(Ordering::Relaxed));
        g(&mut s, "banditx_bytes_in", "Bytes from clients", self.bytes_in.load(Ordering::Relaxed));
        g(&mut s, "banditx_bytes_out", "Bytes to clients", self.bytes_out.load(Ordering::Relaxed));
        g(&mut s, "banditx_fcgi_pool_hits", "FastCGI pool hits", self.fcgi_pool_hits.load(Ordering::Relaxed));
        g(&mut s, "banditx_fcgi_pool_misses", "FastCGI pool misses", self.fcgi_pool_misses.load(Ordering::Relaxed));
        g(&mut s, "banditx_upstream_errors", "Upstream errors", self.upstream_errors.load(Ordering::Relaxed));
        g(&mut s, "banditx_health_check_failures", "Health check failures", self.health_check_failures.load(Ordering::Relaxed));

        s.push_str(&format!(
            "# HELP banditx_active_conns Active connections\n# TYPE banditx_active_conns gauge\nbanditx_active_conns {}\n",
            self.active_conns.load(Ordering::Relaxed)
        ));

        s.push_str("# HELP banditx_upstream_healthy Upstream health (1=up, 0=down)\n# TYPE banditx_upstream_healthy gauge\n");
        for route in routes {
            if let Some(b) = balancers.get(&route.prefix) {
                for (addr, ok) in b.snapshot() {
                    s.push_str(&format!(
                        "banditx_upstream_healthy{{route=\"{}\",addr=\"{}\"}} {}\n",
                        route.prefix, addr, if ok { 1 } else { 0 }
                    ));
                }
            }
        }
        s
    }
}

// ============================================================
//  ACCESS LOG
// ============================================================

struct LogEntry {
    peer: SocketAddr,
    method: String,
    target: String,
    version: String,
    status: u16,
    bytes: u64,
    ua: String,
    duration_ms: u64,
}

struct AccessLog {
    tx: Option<mpsc::Sender<LogEntry>>,
    dropped: Arc<AtomicU64>,
}

impl AccessLog {
    fn start(cfg: Option<&AccessLogCfg>) -> Self {
        let Some(cfg) = cfg else {
            return Self { tx: None, dropped: Arc::new(AtomicU64::new(0)) };
        };
        let (path, format) = match cfg {
            AccessLogCfg::Path(p) => (Some(p.clone()), LogFormat::Combined),
            AccessLogCfg::Full { path, format } => (path.clone(), *format),
        };

        let (tx, mut rx) = mpsc::channel::<LogEntry>(4096);
        let dropped = Arc::new(AtomicU64::new(0));

        tokio::spawn(async move {
            let mut file = match &path {
                Some(p) => match tokio::fs::OpenOptions::new()
                    .create(true).append(true).open(p).await
                {
                    Ok(f) => Some(BufWriter::new(f)),
                    Err(e) => { eprintln!("access_log open {p}: {e}"); None }
                },
                None => None,
            };
            let mut stdout = tokio::io::stdout();

            while let Some(entry) = rx.recv().await {
                let line = match format {
                    LogFormat::Combined => format_combined(&entry),
                    LogFormat::Json => format_json(&entry),
                };
                let r = if let Some(f) = file.as_mut() {
                    f.write_all(line.as_bytes()).await
                } else {
                    stdout.write_all(line.as_bytes()).await
                };
                if let Err(e) = r {
                    eprintln!("access_log write: {e}");
                }
            }
        });

        Self { tx: Some(tx), dropped }
    }

    fn log(&self, peer: SocketAddr, method: &str, target: &str, version: &str,
           status: u16, bytes: u64, ua: &str, duration_ms: u64) {
        let Some(tx) = &self.tx else { return; };
        let entry = LogEntry {
            peer,
            method: method.to_string(),
            target: target.to_string(),
            version: version.to_string(),
            status, bytes,
            ua: ua.to_string(),
            duration_ms,
        };
        if tx.try_send(entry).is_err() {
            let n = self.dropped.fetch_add(1, Ordering::Relaxed) + 1;
            if n == 1 || n % 1000 == 0 {
                eprintln!("access_log: dropped {n} entries (queue full)");
            }
        }
    }
}

fn format_combined(e: &LogEntry) -> String {
    let ts = chrono::Local::now().format("%d/%b/%Y:%H:%M:%S %z");
    format!(
        "{ip} - - [{ts}] \"{method} {target} {version}\" {status} {bytes} \"-\" \"{ua}\"\n",
        ip = e.peer.ip(),
        ts = ts,
        method = e.method,
        target = e.target,
        version = e.version,
        status = e.status,
        bytes = e.bytes,
        ua = e.ua,
    )
}

fn format_json(e: &LogEntry) -> String {
    let ts = chrono::Local::now().to_rfc3339();
    format!(
        "{{\"time\":\"{ts}\",\"remote_addr\":\"{ip}\",\"method\":\"{m}\",\"uri\":\"{u}\",\"proto\":\"{p}\",\"status\":{s},\"bytes\":{b},\"ua\":\"{ua}\",\"duration_ms\":{d}}}\n",
        ts = ts,
        ip = e.peer.ip(),
        m = json_escape(&e.method),
        u = json_escape(&e.target),
        p = json_escape(&e.version),
        s = e.status,
        b = e.bytes,
        ua = json_escape(&e.ua),
        d = e.duration_ms,
    )
}

fn json_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out
}

// ============================================================
//  БАЛАНСЕР
// ============================================================

struct UpstreamState {
    addr: String,
    healthy: AtomicBool,
    fail_streak: AtomicUsize,
    active_conns: AtomicUsize,
}

struct Balancer {
    strategy: Strategy,
    upstreams: Vec<Arc<UpstreamState>>,
    rr: AtomicUsize,
}

impl Balancer {
    fn new(strategy: Strategy, addresses: Vec<String>) -> Self {
        let upstreams = addresses.into_iter()
            .map(|addr| Arc::new(UpstreamState {
                addr,
                healthy: AtomicBool::new(true),
                fail_streak: AtomicUsize::new(0),
                active_conns: AtomicUsize::new(0),
            }))
            .collect();
        Self { strategy, upstreams, rr: AtomicUsize::new(0) }
    }

    fn pick(&self, client_ip: IpAddr) -> Option<Arc<UpstreamState>> {
        if self.upstreams.is_empty() { return None; }

        let healthy: Vec<Arc<UpstreamState>> = self.upstreams.iter()
            .filter(|u| u.healthy.load(Ordering::Relaxed))
            .cloned()
            .collect();
        if healthy.is_empty() { return None; }

        match self.strategy {
            Strategy::RoundRobin => {
                let i = self.rr.fetch_add(1, Ordering::Relaxed);
                Some(healthy[i % healthy.len()].clone())
            }
            Strategy::LeastConn => {
                let min_active = healthy.iter()
                    .map(|u| u.active_conns.load(Ordering::Relaxed))
                    .min()
                    .unwrap_or(0);
                let candidates: Vec<&Arc<UpstreamState>> = healthy.iter()
                    .filter(|u| u.active_conns.load(Ordering::Relaxed) == min_active)
                    .collect();
                let i = self.rr.fetch_add(1, Ordering::Relaxed);
                Some(candidates[i % candidates.len()].clone())
            }
            Strategy::IpHash => {
                let mut h: u64 = 1469598103934665603;
                for b in client_ip.to_string().as_bytes() {
                    h ^= *b as u64;
                    h = h.wrapping_mul(1099511628211);
                }
                Some(healthy[(h as usize) % healthy.len()].clone())
            }
        }
    }

    fn mark_fail(&self, addr: &str, metrics: &Metrics) {
        for u in &self.upstreams {
            if u.addr == addr {
                let streak = u.fail_streak.fetch_add(1, Ordering::Relaxed) + 1;
                if streak >= HEALTH_FAIL_THRESHOLD as usize {
                    if u.healthy.swap(false, Ordering::Relaxed) {
                        eprintln!("banditx: upstream {} marked DOWN", addr);
                    }
                    metrics.health_check_failures.fetch_add(1, Ordering::Relaxed);
                }
                return;
            }
        }
    }

    fn mark_ok(&self, addr: &str) {
        for u in &self.upstreams {
            if u.addr == addr {
                u.fail_streak.store(0, Ordering::Relaxed);
                if !u.healthy.swap(true, Ordering::Relaxed) {
                    eprintln!("banditx: upstream {} marked UP", addr);
                }
                return;
            }
        }
    }

    fn snapshot(&self) -> Vec<(String, bool)> {
        self.upstreams.iter()
            .map(|u| (u.addr.clone(), u.healthy.load(Ordering::Relaxed)))
            .collect()
    }
}

// ============================================================
//  HEALTH CHECKS
// ============================================================

fn spawn_health_checks(app: Arc<App>) {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(HEALTH_INTERVAL).await;
            let cfg = app.cfg.load_full();
            for route in &cfg.routes {
                let balancer = match app.balancers.get(&route.prefix) {
                    Some(b) => b.clone(),
                    None => continue,
                };
                for u in &balancer.upstreams {
                    let addr = u.addr.clone();
                    let bal = balancer.clone();
                    let metrics = app.metrics.clone();
                    tokio::spawn(async move {
                        let ok = matches!(
                            timeout(HEALTH_TIMEOUT, TcpStream::connect(&addr)).await,
                            Ok(Ok(_))
                        );
                        if ok {
                            bal.mark_ok(&addr);
                        } else {
                            bal.mark_fail(&addr, &metrics);
                        }
                    });
                }
            }
        }
    });
}

// ============================================================
//  FASTCGI POOL
// ============================================================

struct FcgiPool {
    addr: String,
    idle: Mutex<Vec<TcpStream>>,
}

impl FcgiPool {
    fn new(addr: String) -> Self {
        Self { addr, idle: Mutex::new(Vec::new()) }
    }

    async fn get(&self, metrics: &Metrics) -> io::Result<TcpStream> {
        loop {
            let popped = self.idle.lock().unwrap().pop();
            match popped {
                Some(s) => {
                    let mut b = [0u8; 1];
                    match s.try_read(&mut b) {
                        Ok(0) => continue,
                        Ok(_) => continue,
                        Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                            metrics.fcgi_pool_hits.fetch_add(1, Ordering::Relaxed);
                            return Ok(s);
                        }
                        Err(_) => continue,
                    }
                }
                None => {
                    let s = timeout(CONNECT_TIMEOUT, TcpStream::connect(&self.addr)).await??;
                    s.set_nodelay(true).ok();
                    metrics.fcgi_pool_misses.fetch_add(1, Ordering::Relaxed);
                    return Ok(s);
                }
            }
        }
    }

    fn put(&self, s: TcpStream) {
        let mut g = self.idle.lock().unwrap();
        if g.len() < FCGI_IDLE_MAX { g.push(s); }
    }
}

struct PoolRegistry {
    fcgi: Mutex<HashMap<String, Arc<FcgiPool>>>,
}

impl PoolRegistry {
    fn new() -> Self { Self { fcgi: Mutex::new(HashMap::new()) } }

    fn fcgi(&self, addr: &str) -> Arc<FcgiPool> {
        let mut g = self.fcgi.lock().unwrap();
        g.entry(addr.to_string())
            .or_insert_with(|| Arc::new(FcgiPool::new(addr.to_string())))
            .clone()
    }
}

// ============================================================
//  APP
// ============================================================

struct App {
    cfg: ArcSwap<Config>,
    metrics: Arc<Metrics>,
    pools: Arc<PoolRegistry>,
    access_log: Arc<AccessLog>,
    cfg_path: String,
    balancers: HashMap<String, Arc<Balancer>>,
}

fn build_balancers(cfg: &Config) -> HashMap<String, Arc<Balancer>> {
    let mut m = HashMap::new();
    for r in &cfg.routes {
        m.insert(r.prefix.clone(),
                 Arc::new(Balancer::new(r.strategy, r.upstreams.clone())));
    }
    m
}

// ============================================================
//  ФРЕЙМИНГ
// ============================================================

#[derive(Debug, Clone, Copy)]
enum Framing {
    None,
    ContentLength(usize),
    Chunked,
    UntilClose,
}

// ============================================================
//  TLS
// ============================================================

fn build_tls_acceptor() -> io::Result<TlsAcceptor> {
    use rcgen::{generate_simple_self_signed, CertifiedKey};
    use tokio_rustls::rustls::pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer};

    let CertifiedKey { cert, key_pair } = generate_simple_self_signed(vec![
        "localhost".to_string(), "127.0.0.1".to_string(),
    ])
    .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("cert: {e}")))?;

    let cert_der = cert.der().clone();
    let key_der = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key_pair.serialize_der()));

    let mut cfg = tokio_rustls::rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert_der], key_der)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("tls: {e}")))?;
    cfg.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    Ok(TlsAcceptor::from(Arc::new(cfg)))
}

// ============================================================
//  MAIN
// ============================================================

#[tokio::main]
async fn main() -> io::Result<()> {
    let cfg_path = std::env::args().nth(1).unwrap_or_else(|| "banditx.yaml".to_string());
    let cfg = load_config(&cfg_path)?;

    let balancers = build_balancers(&cfg);
    let app = Arc::new(App {
        cfg: ArcSwap::from_pointee(cfg.clone()),
        metrics: Arc::new(Metrics::new()),
        pools: Arc::new(PoolRegistry::new()),
        access_log: Arc::new(AccessLog::start(cfg.server.access_log.as_ref())),
        cfg_path: cfg_path.clone(),
        balancers,
    });

    println!("banditx v0.3.3 | http://{} | {} route(s)",
             cfg.server.listen, cfg.routes.len());

    spawn_config_watcher(app.clone());
    spawn_health_checks(app.clone());

    let shutdown = Arc::new(Notify::new());
    {
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            let _ = tokio::signal::ctrl_c().await;
            println!("\nbanditx: shutdown requested");
            shutdown.notify_waiters();
        });
    }

    if let Some(tls_addr) = &cfg.server.tls_listen {
        let acceptor = build_tls_acceptor()?;
        let listener = TcpListener::bind(tls_addr).await?;
        println!("                https://{tls_addr} (self-signed, h2 + http/1.1)");
        let app = app.clone();
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = shutdown.notified() => return,
                    r = listener.accept() => {
                        let (sock, peer) = match r { Ok(x) => x, Err(_) => continue };
                        let acceptor = acceptor.clone();
                        let app = app.clone();
                        tokio::spawn(async move {
                            let tls_stream = match acceptor.accept(sock).await { Ok(s) => s, Err(_) => return };
                            let alpn = tls_stream.get_ref().1.alpn_protocol().map(|p| p.to_vec());
                            match alpn.as_deref() {
                                Some(b"h2") => { let _ = serve_h2(tls_stream, peer, app).await; }
                                _ => { let _ = serve_h1(tls_stream, peer, app).await; }
                            }
                        });
                    }
                }
            }
        });
    }

    for r in &cfg.routes {
        let k = match r.kind { RouteKind::Http => "http", RouteKind::Fastcgi => "fcgi" };
        let strat = match r.strategy { Strategy::RoundRobin => "rr", Strategy::LeastConn => "lc", Strategy::IpHash => "ih" };
        println!("  {} -> {:?} [{} / {}]", r.prefix, r.upstreams, k, strat);
    }
    println!("  metrics: http://{}/_banditx/metrics", cfg.server.listen);

    let listener = TcpListener::bind(&cfg.server.listen).await?;
    loop {
        tokio::select! {
            _ = shutdown.notified() => {
                println!("banditx: stopped accepting new connections");
                tokio::time::sleep(Duration::from_millis(300)).await;
                return Ok(());
            }
            r = listener.accept() => {
                let (client, peer) = match r { Ok(x) => x, Err(_) => continue };
                let app = app.clone();
                app.metrics.active_conns.fetch_add(1, Ordering::Relaxed);
                tokio::spawn(async move {
                    let _ = serve_h1(client, peer, app.clone()).await;
                    app.metrics.active_conns.fetch_sub(1, Ordering::Relaxed);
                });
            }
        }
    }
}

// ============================================================
//  HOT RELOAD
// ============================================================

fn spawn_config_watcher(app: Arc<App>) {
    use notify::{recommended_watcher, RecursiveMode, Watcher};

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<()>();
    let path = app.cfg_path.clone();

    let mut watcher = match recommended_watcher(move |res: notify::Result<notify::Event>| {
        if let Ok(ev) = res {
            let interesting = ev.paths.iter().any(|p| {
                let s = p.to_string_lossy();
                s.ends_with(".yaml") || s.ends_with(".yml")
            });
            if interesting { let _ = tx.send(()); }
        }
    }) {
        Ok(w) => w,
        Err(e) => { eprintln!("watcher init: {e}"); return; }
    };

    let watch_dir = std::path::Path::new(&path)
        .parent().map(|p| p.to_path_buf())
        .unwrap_or_else(|| std::path::PathBuf::from("."));
    if let Err(e) = watcher.watch(&watch_dir, RecursiveMode::NonRecursive) {
        eprintln!("watcher watch: {e}");
        return;
    }

    tokio::spawn(async move {
        let _keep = watcher;
        while rx.recv().await.is_some() {
            tokio::time::sleep(Duration::from_millis(200)).await;
            while rx.try_recv().is_ok() {}
            match load_config(&path) {
                Ok(new_cfg) => {
                    app.cfg.store(Arc::new(new_cfg.clone()));
                    println!("banditx: config reloaded ({} route(s))", new_cfg.routes.len());
                }
                Err(e) => eprintln!("banditx: reload failed, keeping old config: {e}"),
            }
        }
    });
}

// ============================================================
//  HTTP/1.1 SERVE LOOP
// ============================================================

async fn serve_h1<S>(stream: S, peer: SocketAddr, app: Arc<App>) -> io::Result<()>
where S: AsyncRead + AsyncWrite + Unpin + Send + 'static {
    let (r, mut cw) = tokio::io::split(stream);
    let mut cr = BufReader::new(r);

    loop {
        let cfg = app.cfg.load_full();
        let req_raw = match read_header_block(&mut cr).await? { Some(b) => b, None => return Ok(()) };
        let parsed = match parse_request(&req_raw) {
            Ok(p) => p,
            Err(_) => {
                cw.write_all(b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").await.ok();
                return Ok(());
            }
        };

        let started = Instant::now();

        if parsed.path == "/_banditx/metrics" {
            let body = app.metrics.render(&cfg.routes, &app.balancers);
            let out = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4\r\nContent-Length: {}\r\nConnection: keep-alive\r\n\r\n{}",
                body.len(), body
            );
            cw.write_all(out.as_bytes()).await?;
            continue;
        }

        let route = match cfg.pick(&parsed.path) {
            Some(r) => r.clone(),
            None => {
                cw.write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").await.ok();
                return Ok(());
            }
        };

        // ---- статика ----
        if let Some(static_root) = &route.static_root {
            let rel = parsed.path
                .strip_prefix(route.prefix.trim_end_matches('/'))
                .unwrap_or(&parsed.path);
            let rel = if rel.is_empty() { "/" } else { rel };
            match safe_join(static_root, rel) {
                Some(file_path) if file_path.is_file() => {
                    match serve_static(&mut cw, &file_path, &parsed, &req_raw).await {
                        Ok((status, bytes)) => {
                            app.metrics.record_status(status);
                            let ua = parsed.headers_ua_from_raw(&req_raw);
                            let version = format!("HTTP/1.{}", parsed.version_minor);
                            app.access_log.log(
                                peer, &parsed.method, &parsed.full_target(), &version,
                                status, bytes, &ua,
                                started.elapsed().as_millis() as u64,
                            );
                            if parsed.connection_close { return Ok(()); }
                            continue;
                        }
                        Err(e) => {
                            eprintln!("[static] {e}");
                            cw.write_all(b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").await.ok();
                            return Ok(());
                        }
                    }
                }
                _ => {
                    cw.write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n").await.ok();
                    app.metrics.record_status(404);
                    let ua = parsed.headers_ua_from_raw(&req_raw);
                    let version = format!("HTTP/1.{}", parsed.version_minor);
                    app.access_log.log(
                        peer, &parsed.method, &parsed.full_target(), &version,
                        404, 0, &ua,
                        started.elapsed().as_millis() as u64,
                    );
                    if parsed.connection_close { return Ok(()); }
                    continue;
                }
            }
        }

        let balancer = app.balancers.get(&route.prefix).cloned();
        let upstream = match &balancer {
            Some(b) => match b.pick(peer.ip()) {
                Some(u) => u,
                None => {
                    cw.write_all(b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").await.ok();
                    app.metrics.record_status(503);
                    return Ok(());
                }
            },
            None => {
                cw.write_all(b"HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").await.ok();
                return Ok(());
            }
        };
        upstream.active_conns.fetch_add(1, Ordering::Relaxed);

        let addr = upstream.addr.clone();
        let (status, bytes_out, keep) = match route.kind {
            RouteKind::Http => {
                proxy_http_h1(&mut cr, &mut cw, &parsed, &req_raw, peer, &addr, &app).await
                    .unwrap_or_else(|_| {
                        if let Some(b) = &balancer { b.mark_fail(&addr, &app.metrics); }
                        (502, 0, false)
                    })
            }
            RouteKind::Fastcgi => {
                proxy_fastcgi_h1(&mut cr, &mut cw, &parsed, &req_raw, peer, &route, &addr, &app).await
                    .unwrap_or_else(|_| {
                        if let Some(b) = &balancer { b.mark_fail(&addr, &app.metrics); }
                        (502, 0, false)
                    })
            }
        };
        upstream.active_conns.fetch_sub(1, Ordering::Relaxed);
        app.metrics.record_status(status);

        let ua = parsed.headers_ua_from_raw(&req_raw);
        let version = format!("HTTP/1.{}", parsed.version_minor);
        app.access_log.log(
            peer, &parsed.method, &parsed.full_target(), &version,
            status, bytes_out, &ua,
            started.elapsed().as_millis() as u64,
        );

        if !keep || parsed.connection_close { return Ok(()); }
    }
}

// ============================================================
//  HTTP PROXY
// ============================================================

async fn proxy_http_h1<R, W>(
    cr: &mut R, cw: &mut W,
    parsed: &ParsedRequest, req_raw: &[u8],
    peer: SocketAddr, upstream: &str, app: &Arc<App>,
) -> io::Result<(u16, u64, bool)>
where R: AsyncBufReadExt + Unpin, W: AsyncWriteExt + Unpin {
    let up = match timeout(CONNECT_TIMEOUT, TcpStream::connect(upstream)).await {
        Ok(Ok(s)) => s,
        _ => {
            app.metrics.upstream_errors.fetch_add(1, Ordering::Relaxed);
            cw.write_all(b"HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").await.ok();
            return Ok((502, 0, false));
        }
    };
    up.set_nodelay(true).ok();
    let (ur, mut uw) = up.into_split();
    let mut ur = BufReader::new(ur);

    let fwd = build_forward_request(parsed, peer, req_raw);
    uw.write_all(&fwd).await?;

    if forward_body(cr, &mut uw, parsed.framing).await.is_err() {
        cw.write_all(b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").await.ok();
        return Ok((400, 0, false));
    }
    uw.flush().await.ok();

    let resp_raw = match read_header_block(&mut ur).await? {
        Some(b) => b,
        None => {
            app.metrics.upstream_errors.fetch_add(1, Ordering::Relaxed);
            cw.write_all(b"HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").await.ok();
            return Ok((502, 0, false));
        }
    };
    let resp = parse_response(&resp_raw).map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "bad resp"))?;

    let upstream_close = resp.connection_close;
    let framing = response_framing(&resp, &parsed.method);
    let close_client = parsed.connection_close || upstream_close || matches!(framing, Framing::UntilClose);

    let out_head = build_response_head(&resp, close_client);
    cw.write_all(&out_head).await?;
    let n = forward_response_body(&mut ur, cw, framing).await.unwrap_or(0);
    Ok((resp.code, n, !close_client))
}

// ============================================================
//  FASTCGI PROXY
// ============================================================

async fn proxy_fastcgi_h1<R, W>(
    cr: &mut R, cw: &mut W,
    parsed: &ParsedRequest, req_raw: &[u8],
    peer: SocketAddr, route: &RouteCfg, addr: &str, app: &Arc<App>,
) -> io::Result<(u16, u64, bool)>
where R: AsyncBufReadExt + Unpin, W: AsyncWriteExt + Unpin {
    let body = read_body_bounded(cr, parsed.framing, MAX_FCGI_BODY).await?;
    let pool = app.pools.fcgi(addr);
    let mut up = match pool.get(&app.metrics).await {
        Ok(s) => s,
        Err(e) => {
            app.metrics.upstream_errors.fetch_add(1, Ordering::Relaxed);
            eprintln!("[fcgi pool] {e}");
            cw.write_all(b"HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").await.ok();
            return Ok((502, 0, false));
        }
    };

    let doc_root = route.doc_root.as_deref().unwrap_or("/");
    let script_filename = format!("{}{}", doc_root.trim_end_matches('/'), parsed.path);

    let mut params = Vec::new();
    let add = |k: &str, v: &str, out: &mut Vec<u8>| fcgi_encode_nv(out, k.as_bytes(), v.as_bytes());

    add("GATEWAY_INTERFACE", "CGI/1.1", &mut params);
    add("SERVER_SOFTWARE", "banditx/0.3.2", &mut params);
    add("SERVER_PROTOCOL", "HTTP/1.1", &mut params);
    add("REQUEST_METHOD", &parsed.method, &mut params);
    let uri = if parsed.query.is_empty() { parsed.path.clone() }
        else { format!("{}?{}", parsed.path, parsed.query) };
    add("REQUEST_URI", &uri, &mut params);
    add("QUERY_STRING", &parsed.query, &mut params);
    add("SCRIPT_NAME", &parsed.path, &mut params);
    add("SCRIPT_FILENAME", &script_filename, &mut params);
    add("DOCUMENT_ROOT", doc_root, &mut params);
    add("REMOTE_ADDR", &peer.ip().to_string(), &mut params);
    add("REMOTE_PORT", &peer.port().to_string(), &mut params);
    add("SERVER_ADDR", "127.0.0.1", &mut params);
    add("SERVER_PORT", "8080", &mut params);
    add("CONTENT_LENGTH", &body.len().to_string(), &mut params);

    {
        let mut hdrs = [httparse::EMPTY_HEADER; 96];
        let mut hreq = httparse::Request::new(&mut hdrs);
        if hreq.parse(req_raw).is_ok() {
            for h in hreq.headers.iter() {
                let n = h.name.to_ascii_uppercase().replace('-', "_");
                let v = std::str::from_utf8(h.value).unwrap_or("");
                if n == "CONTENT_TYPE" { add("CONTENT_TYPE", v, &mut params); }
                else if n != "CONTENT_LENGTH" { add(&format!("HTTP_{n}"), v, &mut params); }
            }
        }
    }

    let req_id: u16 = 1;
    let mut begin = Vec::with_capacity(8);
    begin.extend_from_slice(&FCGI_RESPONDER.to_be_bytes());
    begin.push(0);
    begin.extend_from_slice(&[0u8; 5]);

    let mut out = Vec::new();
    fcgi_write_record(&mut out, req_id, FCGI_BEGIN_REQUEST, &begin);
    up.write_all(&out).await?;

    for chunk in params.chunks(65535) {
        let mut b = Vec::new();
        fcgi_write_record(&mut b, req_id, FCGI_PARAMS, chunk);
        up.write_all(&b).await?;
    }
    let mut b = Vec::new();
    fcgi_write_record(&mut b, req_id, FCGI_PARAMS, &[]);
    up.write_all(&b).await?;

    for chunk in body.chunks(65535) {
        let mut b = Vec::new();
        fcgi_write_record(&mut b, req_id, FCGI_STDIN, chunk);
        up.write_all(&b).await?;
    }
    let mut b = Vec::new();
    fcgi_write_record(&mut b, req_id, FCGI_STDIN, &[]);
    up.write_all(&b).await?;
    up.flush().await.ok();

    let mut stdout = Vec::new();
    let mut pool_ok = true;
    let read_result: io::Result<()> = async {
        loop {
            let rec = match timeout(BODY_TIMEOUT, fcgi_read_record(&mut up)).await {
                Ok(Ok(x)) => x,
                Ok(Err(e)) => return Err(e),
                Err(_) => return Err(io::Error::new(io::ErrorKind::TimedOut, "fcgi timeout")),
            };
            let Some((ty, _id, content)) = rec else { return Ok(()); };
            match ty {
                FCGI_STDOUT => stdout.extend_from_slice(&content),
                FCGI_STDERR => {
                    if !content.is_empty() {
                        eprintln!("[fcgi stderr] {}", String::from_utf8_lossy(&content));
                    }
                }
                FCGI_END_REQUEST => return Ok(()),
                _ => {}
            }
        }
    }.await;

    if let Err(e) = read_result { eprintln!("[fcgi read] {e}"); pool_ok = false; }
    if pool_ok { pool.put(up); }

    let (cgi_headers, cgi_body) = split_cgi_response(&stdout);
    let mut status_code: u16 = 200;
    let mut status_line = "200 OK".to_string();
    let mut http_headers: Vec<(String, String)> = Vec::new();

    for (n, v) in cgi_headers {
        if n.eq_ignore_ascii_case("status") {
            status_line = v.clone();
            if let Some(c) = v.split_whitespace().next().and_then(|s| s.parse::<u16>().ok()) {
                status_code = c;
            }
            continue;
        }
        if n.eq_ignore_ascii_case("content-length") { continue; }
        if is_hop_by_hop(&n) { continue; }
        http_headers.push((n, v));
    }
    http_headers.push(("Content-Length".to_string(), cgi_body.len().to_string()));

    let mut out = format!("HTTP/1.1 {}\r\n", status_line);
    for (n, v) in &http_headers {
        out.push_str(n); out.push_str(": "); out.push_str(v); out.push_str("\r\n");
    }
    out.push_str(if parsed.connection_close { "Connection: close\r\n" } else { "Connection: keep-alive\r\n" });
    out.push_str("\r\n");
    cw.write_all(out.as_bytes()).await?;
    cw.write_all(&cgi_body).await?;

    Ok((status_code, cgi_body.len() as u64, !parsed.connection_close))
}

fn split_cgi_response(raw: &[u8]) -> (Vec<(String, String)>, Vec<u8>) {
    let idx = find_subslice(raw, b"\r\n\r\n").map(|i| (i, 4))
        .or_else(|| find_subslice(raw, b"\n\n").map(|i| (i, 2)));
    let Some((i, sep)) = idx else { return (Vec::new(), raw.to_vec()); };
    let header_block = &raw[..i];
    let body = raw[i + sep..].to_vec();
    let mut headers = Vec::new();
    for line in header_block.split(|&b| b == b'\n') {
        let line = line.strip_suffix(b"\r").unwrap_or(line);
        if line.is_empty() { continue; }
        if let Some(c) = line.iter().position(|&b| b == b':') {
            let name = String::from_utf8_lossy(&line[..c]).trim().to_string();
            let value = String::from_utf8_lossy(&line[c + 1..]).trim().to_string();
            headers.push((name, value));
        }
    }
    (headers, body)
}

fn find_subslice(hay: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || hay.len() < needle.len() { return None; }
    hay.windows(needle.len()).position(|w| w == needle)
}

fn fcgi_write_record(out: &mut Vec<u8>, req_id: u16, ty: u8, content: &[u8]) {
    let clen = content.len() as u16;
    let pad = (8 - (clen as usize % 8)) % 8;
    out.push(FCGI_VERSION_1);
    out.push(ty);
    out.extend_from_slice(&req_id.to_be_bytes());
    out.extend_from_slice(&clen.to_be_bytes());
    out.push(pad as u8);
    out.push(0);
    out.extend_from_slice(content);
    out.extend(std::iter::repeat(0u8).take(pad));
}

fn fcgi_encode_nv(out: &mut Vec<u8>, name: &[u8], value: &[u8]) {
    let put = |n: usize, out: &mut Vec<u8>| {
        if n < 128 { out.push(n as u8); }
        else { out.extend_from_slice(&((n as u32) | 0x8000_0000).to_be_bytes()); }
    };
    put(name.len(), out);
    put(value.len(), out);
    out.extend_from_slice(name);
    out.extend_from_slice(value);
}

async fn fcgi_read_record<R: AsyncReadExt + Unpin>(
    r: &mut R,
) -> io::Result<Option<(u8, u16, Vec<u8>)>> {
    let mut hdr = [0u8; 8];
    match r.read_exact(&mut hdr).await {
        Ok(_) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }
    let ty = hdr[1];
    let req_id = u16::from_be_bytes([hdr[2], hdr[3]]);
    let clen = u16::from_be_bytes([hdr[4], hdr[5]]) as usize;
    let pad = hdr[6] as usize;
    let mut content = vec![0u8; clen];
    if clen > 0 { r.read_exact(&mut content).await?; }
    if pad > 0 { let mut p = vec![0u8; pad]; r.read_exact(&mut p).await?; }
    Ok(Some((ty, req_id, content)))
}

// ============================================================
//  HTTP/2 SERVER
// ============================================================

async fn serve_h2<S>(stream: S, peer: SocketAddr, app: Arc<App>) -> io::Result<()>
where S: AsyncRead + AsyncWrite + Unpin + Send + 'static {
    let mut conn = h2_server::Builder::new()
        .max_concurrent_streams(256)
        .initial_window_size(1024 * 1024)
        .initial_connection_window_size(16 * 1024 * 1024)
        .handshake(stream).await
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("h2: {e}")))?;

    while let Some(r) = conn.accept().await {
        let (req, respond) = match r { Ok(x) => x, Err(e) => { eprintln!("[{peer}] h2 accept: {e}"); break; } };
        let app = app.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_h2_stream(req, respond, peer, app).await {
                eprintln!("[{peer}] h2 stream: {e}");
            }
        });
    }
    Ok(())
}

async fn handle_h2_stream(
    req: http::Request<h2::RecvStream>,
    respond: h2::server::SendResponse<Bytes>,
    peer: SocketAddr,
    app: Arc<App>,
) -> io::Result<()> {
    let started = Instant::now();

    let ua = req.headers()
        .get("user-agent")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    let method = req.method().to_string();
    let path = req.uri().path().to_string();
    let query = req.uri().query().unwrap_or("").to_string();
    let target = if query.is_empty() { path.clone() } else { format!("{path}?{query}") };

    let result = handle_h2_stream_inner(req, respond, peer, app.clone()).await;

    let (status, bytes) = match &result {
        Ok((s, b)) => (*s, *b),
        Err(_) => (500u16, 0u64),
    };
    app.metrics.record_status(status);
    app.access_log.log(
        peer, &method, &target, "HTTP/2",
        status, bytes, &ua,
        started.elapsed().as_millis() as u64,
    );

    result.map(|_| ())
}

async fn handle_h2_stream_inner(
    req: http::Request<h2::RecvStream>,
    mut respond: h2::server::SendResponse<Bytes>,
    peer: SocketAddr,
    app: Arc<App>,
) -> io::Result<(u16, u64)> {
    let cfg = app.cfg.load_full();
    let method = req.method().to_string();
    let path = req.uri().path().to_string();
    let query = req.uri().query().unwrap_or("");
    let target = if query.is_empty() { path.clone() } else { format!("{path}?{query}") };

    if path == "/_banditx/metrics" {
        let body = app.metrics.render(&cfg.routes, &app.balancers).into_bytes();
        let n = body.len() as u64;
        send_h2_body(&mut respond, 200, "text/plain; version=0.0.4", &body).await?;
        return Ok((200, n));
    }

    let route = match cfg.pick(&path) {
        Some(r) => r.clone(),
        None => {
            let body = b"not found";
            send_h2_body(&mut respond, 404, "text/plain", body).await?;
            return Ok((404, body.len() as u64));
        }
    };

    if route.kind == RouteKind::Fastcgi {
        let balancer = match app.balancers.get(&route.prefix) {
            Some(b) => b.clone(),
            None => {
                let body = b"no balancer";
                send_h2_body(&mut respond, 502, "text/plain", body).await?;
                return Ok((502, body.len() as u64));
            }
        };
        let upstream = match balancer.pick(peer.ip()) {
            Some(u) => u,
            None => {
                let body = b"all upstreams down";
                send_h2_body(&mut respond, 503, "text/plain", body).await?;
                return Ok((503, body.len() as u64));
            }
        };
        return proxy_fastcgi_h2(req, respond, peer, &route, &upstream, &app).await;
    }

    // ---- статика по h2 ----
    if let Some(static_root) = &route.static_root {
        let rel = path
            .strip_prefix(route.prefix.trim_end_matches('/'))
            .unwrap_or(&path);
        let rel = if rel.is_empty() { "/" } else { rel };
        match safe_join(static_root, rel) {
            Some(file_path) if file_path.is_file() => {
                let meta = tokio::fs::metadata(&file_path).await?;
                let file_len = meta.len();
                serve_static_h2(&mut respond, &file_path, &method, &req).await?;
                return Ok((200, file_len));
            }
            _ => {
                let body = b"not found";
                send_h2_body(&mut respond, 404, "text/plain", body).await?;
                return Ok((404, body.len() as u64));
            }
        }
    }

    let balancer = match app.balancers.get(&route.prefix) {
        Some(b) => b.clone(),
        None => {
            let body = b"no balancer";
            send_h2_body(&mut respond, 502, "text/plain", body).await?;
            return Ok((502, body.len() as u64));
        }
    };
    let upstream = match balancer.pick(peer.ip()) {
        Some(u) => u,
        None => {
            let body = b"all upstreams down";
            send_h2_body(&mut respond, 503, "text/plain", body).await?;
            return Ok((503, body.len() as u64));
        }
    };

    let up = match timeout(CONNECT_TIMEOUT, TcpStream::connect(&upstream.addr)).await {
        Ok(Ok(s)) => s,
        _ => {
            balancer.mark_fail(&upstream.addr, &app.metrics);
            let body = b"upstream down";
            send_h2_body(&mut respond, 502, "text/plain", body).await?;
            return Ok((502, body.len() as u64));
        }
    };
    up.set_nodelay(true).ok();
    let (ur, mut uw) = up.into_split();
    let mut ur = BufReader::new(ur);

    let mut fwd = Vec::with_capacity(512);
    fwd.extend_from_slice(method.as_bytes());
    fwd.push(b' ');
    fwd.extend_from_slice(target.as_bytes());
    fwd.extend_from_slice(b" HTTP/1.1\r\n");

    let original_host = req.uri().authority().map(|a| a.to_string()).unwrap_or_default();

    for (name, value) in req.headers() {
        let n = name.as_str();
        if n.starts_with(':') { continue; }
        if is_hop_by_hop(n) { continue; }
        if n.eq_ignore_ascii_case("host") { continue; }
        fwd.extend_from_slice(n.as_bytes());
        fwd.extend_from_slice(b": ");
        fwd.extend_from_slice(value.as_bytes());
        fwd.extend_from_slice(b"\r\n");
    }
    let host = if original_host.is_empty() { upstream.addr.clone() } else { original_host.clone() };
    fwd.extend_from_slice(b"Host: "); fwd.extend_from_slice(host.as_bytes()); fwd.extend_from_slice(b"\r\n");
    fwd.extend_from_slice(b"X-Real-IP: "); fwd.extend_from_slice(peer.ip().to_string().as_bytes()); fwd.extend_from_slice(b"\r\n");
    fwd.extend_from_slice(b"X-Forwarded-For: "); fwd.extend_from_slice(peer.ip().to_string().as_bytes()); fwd.extend_from_slice(b"\r\n");
    fwd.extend_from_slice(b"X-Forwarded-Proto: https\r\n");
    fwd.extend_from_slice(b"X-Forwarded-Host: "); fwd.extend_from_slice(host.as_bytes()); fwd.extend_from_slice(b"\r\n");
    fwd.extend_from_slice(b"Connection: keep-alive\r\n\r\n");
    uw.write_all(&fwd).await?;

    let mut body = req.into_body();
    while let Some(chunk) = body.data().await {
        let chunk = chunk.map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        uw.write_all(&chunk).await?;
    }
    uw.flush().await.ok();

    let resp_raw = match read_header_block(&mut ur).await? {
        Some(b) => b,
        None => {
            balancer.mark_fail(&upstream.addr, &app.metrics);
            let body = b"no response";
            send_h2_body(&mut respond, 502, "text/plain", body).await?;
            return Ok((502, body.len() as u64));
        }
    };
    let resp = parse_response(&resp_raw).map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "bad resp"))?;
    let framing = response_framing(&resp, &method);

    balancer.mark_ok(&upstream.addr);

    let content_length: Option<u64> = resp.headers.iter()
        .find(|(n, _)| n.eq_ignore_ascii_case("content-length"))
        .and_then(|(_, v)| v.parse().ok());

    let mut h2_resp = Response::builder().status(resp.code).body(())
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
    {
        let hdrs = h2_resp.headers_mut();
        for (n, v) in &resp.headers {
            if is_hop_by_hop(n) { continue; }
            if n.eq_ignore_ascii_case("transfer-encoding") { continue; }
            if let (Ok(name), Ok(val)) = (
                http::header::HeaderName::from_bytes(n.as_bytes()),
                HeaderValue::from_str(v),
            ) { hdrs.append(name, val); }
        }
    }
    let mut send = respond.send_response(h2_resp, false)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
    stream_h1_body_to_h2(&mut ur, &mut send, framing).await?;
    send.send_data(Bytes::new(), true)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

    Ok((resp.code, content_length.unwrap_or(0)))
}

async fn proxy_fastcgi_h2(
    req: http::Request<h2::RecvStream>,
    mut respond: h2::server::SendResponse<Bytes>,
    peer: SocketAddr,
    route: &RouteCfg,
    upstream: &Arc<UpstreamState>,
    app: &Arc<App>,
) -> io::Result<(u16, u64)> {
    let method = req.method().to_string();
    let path = req.uri().path().to_string();
    let query = req.uri().query().unwrap_or("").to_string();

    // 1. Собираем заголовки ДО into_body (иначе req уедет)
    let headers: Vec<(String, String)> = req.headers().iter()
        .filter(|(n, _)| !n.as_str().starts_with(':'))
        .map(|(n, v)| (n.as_str().to_string(), v.to_str().unwrap_or("").to_string()))
        .collect();
    let authority = req.uri().authority().map(|a| a.as_str().to_string());

    // 2. Читаем тело (для FastCGI нужен CONTENT_LENGTH)
    let mut body = Vec::new();
    let mut stream = req.into_body();
    while let Some(chunk) = stream.data().await {
        let chunk = chunk.map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        if body.len() + chunk.len() > MAX_FCGI_BODY {
            let msg = b"body too large";
            send_h2_body(&mut respond, 413, "text/plain", msg).await?;
            return Ok((413, msg.len() as u64));
        }
        body.extend_from_slice(&chunk);
    }

    // 3. Пул FastCGI
    let pool = app.pools.fcgi(&upstream.addr);
    let mut up = match pool.get(&app.metrics).await {
        Ok(s) => s,
        Err(e) => {
            app.metrics.upstream_errors.fetch_add(1, Ordering::Relaxed);
            eprintln!("[fcgi pool h2] {e}");
            let msg = b"fastcgi upstream unavailable";
            send_h2_body(&mut respond, 502, "text/plain", msg).await?;
            return Ok((502, msg.len() as u64));
        }
    };

    // 4. PARAMS
    let doc_root = route.doc_root.as_deref().unwrap_or("/");
    let script_filename = format!("{}{}", doc_root.trim_end_matches('/'), path);

    let mut params = Vec::new();
    let add = |k: &str, v: &str, out: &mut Vec<u8>| fcgi_encode_nv(out, k.as_bytes(), v.as_bytes());

    add("GATEWAY_INTERFACE", "CGI/1.1", &mut params);
    add("SERVER_SOFTWARE", "banditx/0.3.3", &mut params);
    add("SERVER_PROTOCOL", "HTTP/2", &mut params);
    add("REQUEST_METHOD", &method, &mut params);
    let uri = if query.is_empty() { path.clone() } else { format!("{}?{}", path, query) };
    add("REQUEST_URI", &uri, &mut params);
    add("QUERY_STRING", &query, &mut params);
    add("SCRIPT_NAME", &path, &mut params);
    add("SCRIPT_FILENAME", &script_filename, &mut params);
    add("DOCUMENT_ROOT", doc_root, &mut params);
    add("REMOTE_ADDR", &peer.ip().to_string(), &mut params);
    add("REMOTE_PORT", &peer.port().to_string(), &mut params);
    add("SERVER_ADDR", "127.0.0.1", &mut params);
    add("SERVER_PORT", "8443", &mut params);
    add("CONTENT_LENGTH", &body.len().to_string(), &mut params);

    if let Some(auth) = &authority {
        add("HTTP_HOST", auth, &mut params);
        add("SERVER_NAME", auth.split(':').next().unwrap_or(auth), &mut params);
    }

    for (n, v) in &headers {
        let key = n.to_ascii_uppercase().replace('-', "_");
        if key == "CONTENT_TYPE" {
            add("CONTENT_TYPE", v, &mut params);
        } else if key != "CONTENT_LENGTH" && key != "HOST" {
            add(&format!("HTTP_{key}"), v, &mut params);
        }
    }

    // 5. BEGIN_REQUEST + PARAMS + STDIN
    let req_id: u16 = 1;
    let mut begin = Vec::with_capacity(8);
    begin.extend_from_slice(&FCGI_RESPONDER.to_be_bytes());
    begin.push(0);
    begin.extend_from_slice(&[0u8; 5]);

    let mut out = Vec::new();
    fcgi_write_record(&mut out, req_id, FCGI_BEGIN_REQUEST, &begin);
    up.write_all(&out).await?;

    for chunk in params.chunks(65535) {
        let mut b = Vec::new();
        fcgi_write_record(&mut b, req_id, FCGI_PARAMS, chunk);
        up.write_all(&b).await?;
    }
    let mut b = Vec::new();
    fcgi_write_record(&mut b, req_id, FCGI_PARAMS, &[]);
    up.write_all(&b).await?;

    for chunk in body.chunks(65535) {
        let mut b = Vec::new();
        fcgi_write_record(&mut b, req_id, FCGI_STDIN, chunk);
        up.write_all(&b).await?;
    }
    let mut b = Vec::new();
    fcgi_write_record(&mut b, req_id, FCGI_STDIN, &[]);
    up.write_all(&b).await?;
    up.flush().await.ok();

    // 6. Читаем ответ
    let mut stdout = Vec::new();
    let mut pool_ok = true;
    let read_result: io::Result<()> = async {
        loop {
            let rec = match timeout(BODY_TIMEOUT, fcgi_read_record(&mut up)).await {
                Ok(Ok(x)) => x,
                Ok(Err(e)) => return Err(e),
                Err(_) => return Err(io::Error::new(io::ErrorKind::TimedOut, "fcgi timeout")),
            };
            let Some((ty, _id, content)) = rec else { return Ok(()); };
            match ty {
                FCGI_STDOUT => stdout.extend_from_slice(&content),
                FCGI_STDERR => {
                    if !content.is_empty() {
                        eprintln!("[fcgi stderr h2] {}", String::from_utf8_lossy(&content));
                    }
                }
                FCGI_END_REQUEST => return Ok(()),
                _ => {}
            }
        }
    }.await;

    if let Err(e) = read_result {
        eprintln!("[fcgi read h2] {e}");
        pool_ok = false;
    }
    if pool_ok { pool.put(up); }

    // 7. CGI → h2
    let (cgi_headers, cgi_body) = split_cgi_response(&stdout);
    let mut status_code: u16 = 200;
    let mut h2_headers: Vec<(String, String)> = Vec::new();

    for (n, v) in cgi_headers {
        if n.eq_ignore_ascii_case("status") {
            if let Some(c) = v.split_whitespace().next().and_then(|s| s.parse::<u16>().ok()) {
                status_code = c;
            }
            continue;
        }
        if n.eq_ignore_ascii_case("content-length") { continue; }
        if is_hop_by_hop(&n) { continue; }
        h2_headers.push((n, v));
    }

    let mut h2_resp = Response::builder().status(status_code).body(())
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
    {
        let hdrs = h2_resp.headers_mut();
        for (n, v) in &h2_headers {
            if let (Ok(name), Ok(val)) = (
                http::header::HeaderName::from_bytes(n.as_bytes()),
                HeaderValue::from_str(v),
            ) { hdrs.append(name, val); }
        }
        hdrs.insert(
            http::header::CONTENT_LENGTH,
            HeaderValue::from(cgi_body.len()),
        );
    }

    let end_stream = cgi_body.is_empty();
    let mut send = respond.send_response(h2_resp, end_stream)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
    if !end_stream {
        h2_send(&mut send, &cgi_body).await?;
        send.send_data(Bytes::new(), true)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
    }

    Ok((status_code, cgi_body.len() as u64))
}

async fn send_h2_body(
    respond: &mut h2::server::SendResponse<Bytes>, status: u16,
    content_type: &str, body: &[u8],
) -> io::Result<()> {
    let resp = Response::builder()
        .status(status)
        .header("content-type", content_type)
        .header("content-length", body.len())
        .body(()).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
    let mut send = respond.send_response(resp, body.is_empty())
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
    if !body.is_empty() {
        send.send_data(Bytes::copy_from_slice(body), true)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
    }
    Ok(())
}

async fn stream_h1_body_to_h2<R>(
    r: &mut R, send: &mut h2::SendStream<Bytes>, framing: Framing,
) -> io::Result<()> where R: AsyncBufReadExt + Unpin {
    let mut buf = [0u8; 16 * 1024];
    match framing {
        Framing::None => Ok(()),
        Framing::ContentLength(n) => {
            let mut rem = n;
            while rem > 0 {
                let want = rem.min(buf.len());
                let got = r.read(&mut buf[..want]).await?;
                if got == 0 { return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "eof")); }
                h2_send(send, &buf[..got]).await?;
                rem -= got;
            }
            Ok(())
        }
        Framing::Chunked => loop {
            let mut line = Vec::new();
            let n = r.read_until(b'\n', &mut line).await?;
            if n == 0 { return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "eof")); }
            let s = std::str::from_utf8(&line).map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "chunk"))?;
            let hex = s.split(';').next().unwrap_or("").trim();
            let size = usize::from_str_radix(hex, 16).map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "size"))?;
            if size == 0 {
                loop {
                    let mut t = Vec::new();
                    let n = r.read_until(b'\n', &mut t).await?;
                    if n == 0 { return Ok(()); }
                    if t == b"\r\n" || t == b"\n" { return Ok(()); }
                }
            }
            let mut rem = size;
            while rem > 0 {
                let want = rem.min(buf.len());
                let got = r.read(&mut buf[..want]).await?;
                if got == 0 { return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "eof")); }
                h2_send(send, &buf[..got]).await?;
                rem -= got;
            }
            let mut crlf = [0u8; 2];
            r.read_exact(&mut crlf).await?;
        },
        Framing::UntilClose => loop {
            let n = r.read(&mut buf).await?;
            if n == 0 { return Ok(()); }
            h2_send(send, &buf[..n]).await?;
        },
    }
}

async fn h2_send(send: &mut h2::SendStream<Bytes>, data: &[u8]) -> io::Result<()> {
    let mut data = data;
    while !data.is_empty() {
        send.reserve_capacity(data.len());
        let cap = futures_util::future::poll_fn(|cx| send.poll_capacity(cx)).await;
        let cap = match cap {
            Some(Ok(n)) => n,
            Some(Err(e)) => return Err(io::Error::new(io::ErrorKind::Other, e)),
            None => return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "h2 closed")),
        };
        if cap == 0 { continue; }
        let n = data.len().min(cap);
        send.send_data(Bytes::copy_from_slice(&data[..n]), false)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        data = &data[n..];
    }
    Ok(())
}

// ============================================================
//  PARSING
// ============================================================

struct ParsedRequest {
    method: String,
    path: String,
    query: String,
    version_minor: u8,
    framing: Framing,
    connection_close: bool,
}

impl ParsedRequest {
    fn full_target(&self) -> String {
        if self.query.is_empty() { self.path.clone() }
        else { format!("{}?{}", self.path, self.query) }
    }
    fn headers_ua_from_raw(&self, raw: &[u8]) -> String {
        let mut headers = [httparse::EMPTY_HEADER; 96];
        let mut req = httparse::Request::new(&mut headers);
        if req.parse(raw).is_err() { return String::new(); }
        for h in req.headers.iter() {
            if h.name.eq_ignore_ascii_case("user-agent") {
                return String::from_utf8_lossy(h.value).to_string();
            }
        }
        String::new()
    }
}

fn parse_request(raw: &[u8]) -> io::Result<ParsedRequest> {
    let mut headers = [httparse::EMPTY_HEADER; 96];
    let mut req = httparse::Request::new(&mut headers);
    match req.parse(raw) {
        Ok(httparse::Status::Complete(_)) => {}
        _ => return Err(io::Error::new(io::ErrorKind::InvalidData, "bad request")),
    }
    let method = req.method.unwrap_or("").to_string();
    let target = req.path.unwrap_or("/");
    let (path, query) = match target.split_once('?') {
        Some((p, q)) => (p.to_string(), q.to_string()),
        None => (target.to_string(), String::new()),
    };
    let version_minor = req.version.unwrap_or(1);

    let mut cl: Option<usize> = None;
    let mut chunked = false;
    let mut connection_close = false;

    for h in req.headers.iter() {
        let n = h.name;
        let v = std::str::from_utf8(h.value).unwrap_or("");
        if n.eq_ignore_ascii_case("content-length") { cl = v.trim().parse().ok(); }
        else if n.eq_ignore_ascii_case("transfer-encoding") { if v.to_ascii_lowercase().contains("chunked") { chunked = true; } }
        else if n.eq_ignore_ascii_case("connection") { if v.to_ascii_lowercase().contains("close") { connection_close = true; } }
    }
    if chunked && cl.is_some() { return Err(io::Error::new(io::ErrorKind::InvalidData, "CL+TE")); }
    let framing = if chunked { Framing::Chunked } else {
        match cl { Some(0) | None => Framing::None, Some(n) => Framing::ContentLength(n) }
    };
    Ok(ParsedRequest { method, path, query, version_minor, framing, connection_close })
}

struct ParsedResponse {
    code: u16,
    reason: String,
    headers: Vec<(String, String)>,
    connection_close: bool,
}

fn parse_response(raw: &[u8]) -> io::Result<ParsedResponse> {
    let mut headers = [httparse::EMPTY_HEADER; 96];
    let mut resp = httparse::Response::new(&mut headers);
    match resp.parse(raw) {
        Ok(httparse::Status::Complete(_)) => {}
        _ => return Err(io::Error::new(io::ErrorKind::InvalidData, "bad response")),
    }
    let code = resp.code.unwrap_or(0);
    let reason = resp.reason.unwrap_or("").to_string();
    let mut hs = Vec::with_capacity(resp.headers.len());
    let mut connection_close = false;
    for h in resp.headers.iter() {
        let n = h.name.to_string();
        let v = std::str::from_utf8(h.value).unwrap_or("").to_string();
        if n.eq_ignore_ascii_case("connection") && v.to_ascii_lowercase().contains("close") {
            connection_close = true;
        }
        hs.push((n, v));
    }
    Ok(ParsedResponse { code, reason, headers: hs, connection_close })
}

fn response_framing(resp: &ParsedResponse, method: &str) -> Framing {
    if method.eq_ignore_ascii_case("HEAD") { return Framing::None; }
    if resp.code == 204 || resp.code == 304 || (100..200).contains(&resp.code) { return Framing::None; }
    let mut cl: Option<usize> = None;
    let mut chunked = false;
    for (n, v) in &resp.headers {
        if n.eq_ignore_ascii_case("content-length") { cl = v.trim().parse().ok(); }
        else if n.eq_ignore_ascii_case("transfer-encoding") && v.to_ascii_lowercase().contains("chunked") { chunked = true; }
    }
    if chunked { Framing::Chunked } else {
        match cl { Some(0) => Framing::None, Some(n) => Framing::ContentLength(n), None => Framing::UntilClose }
    }
}

fn is_hop_by_hop(name: &str) -> bool {
    let ln = name.to_ascii_lowercase();
    matches!(ln.as_str(),
        "connection" | "keep-alive" | "proxy-authenticate" | "proxy-authorization"
        | "proxy-connection" | "te" | "trailer" | "upgrade")
}

fn build_forward_request(req: &ParsedRequest, peer: SocketAddr, original_raw: &[u8]) -> Vec<u8> {
    let mut headers = [httparse::EMPTY_HEADER; 96];
    let mut hreq = httparse::Request::new(&mut headers);
    let _ = hreq.parse(original_raw);

    let target = {
        let e = original_raw.windows(2).position(|w| w == b"\r\n").unwrap_or(original_raw.len());
        let line = &original_raw[..e];
        let mut parts = line.split(|&b| b == b' ');
        parts.next();
        parts.next().unwrap_or(b"/").to_vec()
    };

    let mut out = Vec::with_capacity(512);
    out.extend_from_slice(req.method.as_bytes());
    out.push(b' ');
    out.extend_from_slice(&target);
    out.extend_from_slice(b" HTTP/1.1\r\n");

    let mut had_xff = false;
    let mut had_xri = false;
    let mut had_host = false;
    let mut host_value = String::new();

    for h in hreq.headers.iter() {
        let n = h.name;
        let v = h.value;
        let ln = n.to_ascii_lowercase();
        if matches!(ln.as_str(),
            "connection" | "keep-alive" | "proxy-authenticate" | "proxy-authorization"
            | "proxy-connection" | "te" | "trailer" | "upgrade") {
            continue;
        }
        if n.eq_ignore_ascii_case("host") {
            host_value = String::from_utf8_lossy(v).to_string();
            had_host = true;
        }
        if n.eq_ignore_ascii_case("x-forwarded-for") {
            out.extend_from_slice(b"X-Forwarded-For: ");
            out.extend_from_slice(v);
            out.extend_from_slice(b", ");
            out.extend_from_slice(peer.ip().to_string().as_bytes());
            out.extend_from_slice(b"\r\n");
            had_xff = true;
            continue;
        }
        if n.eq_ignore_ascii_case("x-real-ip") {
            out.extend_from_slice(b"X-Real-IP: ");
            out.extend_from_slice(peer.ip().to_string().as_bytes());
            out.extend_from_slice(b"\r\n");
            had_xri = true;
            continue;
        }
        out.extend_from_slice(n.as_bytes());
        out.extend_from_slice(b": ");
        out.extend_from_slice(v);
        out.extend_from_slice(b"\r\n");
    }
    if !had_xff {
        out.extend_from_slice(b"X-Forwarded-For: ");
        out.extend_from_slice(peer.ip().to_string().as_bytes());
        out.extend_from_slice(b"\r\n");
    }
    if !had_xri {
        out.extend_from_slice(b"X-Real-IP: ");
        out.extend_from_slice(peer.ip().to_string().as_bytes());
        out.extend_from_slice(b"\r\n");
    }
    out.extend_from_slice(b"X-Forwarded-Proto: http\r\n");
    if had_host {
        out.extend_from_slice(b"X-Forwarded-Host: ");
        out.extend_from_slice(host_value.as_bytes());
        out.extend_from_slice(b"\r\n");
    }
    out.extend_from_slice(b"Connection: keep-alive\r\n\r\n");
    out
}

fn build_response_head(resp: &ParsedResponse, close: bool) -> Vec<u8> {
    let mut out = Vec::with_capacity(256);
    out.extend_from_slice(format!("HTTP/1.1 {} {}\r\n", resp.code, resp.reason).as_bytes());
    for (n, v) in &resp.headers {
        let ln = n.to_ascii_lowercase();
        if matches!(ln.as_str(),
            "connection" | "keep-alive" | "proxy-authenticate" | "proxy-authorization"
            | "proxy-connection" | "te" | "trailer" | "upgrade") { continue; }
        out.extend_from_slice(n.as_bytes());
        out.extend_from_slice(b": ");
        out.extend_from_slice(v.as_bytes());
        out.extend_from_slice(b"\r\n");
    }
    out.extend_from_slice(if close { b"Connection: close\r\n" } else { b"Connection: keep-alive\r\n" });
    out.extend_from_slice(b"\r\n");
    out
}

async fn read_header_block<R: AsyncBufReadExt + Unpin>(r: &mut R) -> io::Result<Option<Vec<u8>>> {
    let mut buf = Vec::with_capacity(2048);
    loop {
        let mut line = Vec::new();
        let n = match timeout(HEADER_TIMEOUT, r.read_until(b'\n', &mut line)).await {
            Ok(Ok(n)) => n,
            Ok(Err(e)) => return Err(e),
            Err(_) => return Err(io::Error::new(io::ErrorKind::TimedOut, "hdr timeout")),
        };
        if n == 0 {
            if buf.is_empty() { return Ok(None); }
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "eof"));
        }
        if buf.len() + line.len() > MAX_HEADER_BYTES {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "hdrs too large"));
        }
        buf.extend_from_slice(&line);
        if line == b"\r\n" || line == b"\n" { return Ok(Some(buf)); }
    }
}

async fn forward_body<R, W>(r: &mut R, w: &mut W, framing: Framing) -> io::Result<()>
where R: AsyncBufReadExt + Unpin, W: AsyncWriteExt + Unpin {
    let mut buf = [0u8; 16 * 1024];
    match framing {
        Framing::None => Ok(()),
        Framing::ContentLength(n) => {
            let mut rem = n;
            while rem > 0 {
                let want = rem.min(buf.len());
                let got = timeout(BODY_TIMEOUT, r.read(&mut buf[..want])).await
                    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "body"))??;
                if got == 0 { return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "eof")); }
                w.write_all(&buf[..got]).await?;
                rem -= got;
            }
            Ok(())
        }
        Framing::Chunked => loop {
            let mut line = Vec::new();
            let n = r.read_until(b'\n', &mut line).await?;
            if n == 0 { return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "eof")); }
            w.write_all(&line).await?;
            let s = std::str::from_utf8(&line).map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "chunk"))?;
            let hex = s.split(';').next().unwrap_or("").trim();
            let size = usize::from_str_radix(hex, 16).map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "size"))?;
            if size == 0 {
                loop {
                    let mut t = Vec::new();
                    let n = r.read_until(b'\n', &mut t).await?;
                    if n == 0 { return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "eof")); }
                    w.write_all(&t).await?;
                    if t == b"\r\n" || t == b"\n" { return Ok(()); }
                }
            }
            let mut rem = size;
            while rem > 0 {
                let want = rem.min(buf.len());
                let got = timeout(BODY_TIMEOUT, r.read(&mut buf[..want])).await
                    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "chunk"))??;
                if got == 0 { return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "eof")); }
                w.write_all(&buf[..got]).await?;
                rem -= got;
            }
            let mut crlf = [0u8; 2];
            r.read_exact(&mut crlf).await?;
            w.write_all(&crlf).await?;
        },
        Framing::UntilClose => Err(io::Error::new(io::ErrorKind::InvalidData, "UntilClose")),
    }
}

async fn read_body_bounded<R: AsyncBufReadExt + Unpin>(
    r: &mut R, framing: Framing, max: usize,
) -> io::Result<Vec<u8>> {
    let mut out = Vec::new();
    let mut buf = [0u8; 16 * 1024];
    match framing {
        Framing::None => Ok(out),
        Framing::ContentLength(n) => {
            if n > max { return Err(io::Error::new(io::ErrorKind::InvalidData, "too large")); }
            out.reserve(n);
            while out.len() < n {
                let want = (n - out.len()).min(buf.len());
                let got = timeout(BODY_TIMEOUT, r.read(&mut buf[..want])).await
                    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "body"))??;
                if got == 0 { return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "eof")); }
                out.extend_from_slice(&buf[..got]);
            }
            Ok(out)
        }
        Framing::Chunked => loop {
            let mut line = Vec::new();
            let n = r.read_until(b'\n', &mut line).await?;
            if n == 0 { return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "eof")); }
            let s = std::str::from_utf8(&line).map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "chunk"))?;
            let hex = s.split(';').next().unwrap_or("").trim();
            let size = usize::from_str_radix(hex, 16).map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "size"))?;
            if size == 0 {
                loop {
                    let mut t = Vec::new();
                    let n = r.read_until(b'\n', &mut t).await?;
                    if n == 0 { return Ok(out); }
                    if t == b"\r\n" || t == b"\n" { return Ok(out); }
                }
            }
            if out.len() + size > max { return Err(io::Error::new(io::ErrorKind::InvalidData, "too large")); }
            let mut rem = size;
            while rem > 0 {
                let want = rem.min(buf.len());
                let got = timeout(BODY_TIMEOUT, r.read(&mut buf[..want])).await
                    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "chunk"))??;
                if got == 0 { return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "eof")); }
                out.extend_from_slice(&buf[..got]);
                rem -= got;
            }
            let mut crlf = [0u8; 2];
            r.read_exact(&mut crlf).await?;
        },
        Framing::UntilClose => Err(io::Error::new(io::ErrorKind::InvalidData, "UntilClose")),
    }
}

async fn forward_response_body<R, W>(r: &mut R, w: &mut W, framing: Framing) -> io::Result<u64>
where R: AsyncBufReadExt + Unpin, W: AsyncWriteExt + Unpin {
    let mut buf = [0u8; 16 * 1024];
    let mut total: u64 = 0;
    match framing {
        Framing::None => Ok(0),
        Framing::ContentLength(n) => {
            let mut rem = n;
            while rem > 0 {
                let want = rem.min(buf.len());
                let got = r.read(&mut buf[..want]).await?;
                if got == 0 { return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "eof")); }
                w.write_all(&buf[..got]).await?;
                total += got as u64;
                rem -= got;
            }
            Ok(total)
        }
        Framing::Chunked => {
            loop {
                let mut line = Vec::new();
                let n = r.read_until(b'\n', &mut line).await?;
                if n == 0 { return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "eof")); }
                w.write_all(&line).await?;
                let s = std::str::from_utf8(&line).map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "chunk"))?;
                let hex = s.split(';').next().unwrap_or("").trim();
                let size = usize::from_str_radix(hex, 16).map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "size"))?;
                if size == 0 {
                    loop {
                        let mut t = Vec::new();
                        let n = r.read_until(b'\n', &mut t).await?;
                        if n == 0 { return Ok(total); }
                        w.write_all(&t).await?;
                        if t == b"\r\n" || t == b"\n" { return Ok(total); }
                    }
                }
                let mut rem = size;
                while rem > 0 {
                    let want = rem.min(buf.len());
                    let got = r.read(&mut buf[..want]).await?;
                    if got == 0 { return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "eof")); }
                    w.write_all(&buf[..got]).await?;
                    total += got as u64;
                    rem -= got;
                }
                let mut crlf = [0u8; 2];
                r.read_exact(&mut crlf).await?;
                w.write_all(&crlf).await?;
            }
        }
        Framing::UntilClose => loop {
            let n = r.read(&mut buf).await?;
            if n == 0 { return Ok(total); }
            w.write_all(&buf[..n]).await?;
            total += n as u64;
        },
    }
}

// ============================================================
//  STATIC FILE SERVING
// ============================================================

fn safe_join(root: &str, url_path: &str) -> Option<std::path::PathBuf> {
    use std::path::Component;
    let mut p = std::path::PathBuf::from(root);
    let url = std::path::Path::new(url_path);
    for c in url.components() {
        match c {
            Component::Normal(seg) => {
                let s = seg.to_string_lossy();
                if s.is_empty() || s.contains('\0') { return None; }
                p.push(seg);
            }
            Component::RootDir | Component::CurDir => {}
            Component::ParentDir | Component::Prefix(_) => return None,
        }
    }
    Some(p)
}

fn mime_of(p: &std::path::Path) -> &'static str {
    match p.extension().and_then(|e| e.to_str()).map(|s| s.to_ascii_lowercase()).as_deref() {
        Some("html") | Some("htm") => "text/html; charset=utf-8",
        Some("css") => "text/css; charset=utf-8",
        Some("js") | Some("mjs") => "application/javascript",
        Some("json") => "application/json",
        Some("png") => "image/png",
        Some("jpg") | Some("jpeg") => "image/jpeg",
        Some("gif") => "image/gif",
        Some("svg") => "image/svg+xml",
        Some("webp") => "image/webp",
        Some("ico") => "image/x-icon",
        Some("wasm") => "application/wasm",
        Some("txt") => "text/plain; charset=utf-8",
        Some("xml") => "application/xml",
        Some("pdf") => "application/pdf",
        Some("woff") => "font/woff",
        Some("woff2") => "font/woff2",
        Some("ttf") => "font/ttf",
        Some("otf") => "font/otf",
        Some("mp4") => "video/mp4",
        Some("webm") => "video/webm",
        Some("mp3") => "audio/mpeg",
        Some("zip") => "application/zip",
        _ => "application/octet-stream",
    }
}

fn format_http_date(t: Option<std::time::SystemTime>) -> String {
    let t = t.unwrap_or(std::time::SystemTime::UNIX_EPOCH);
    let dt: chrono::DateTime<chrono::Utc> = t.into();
    dt.format("%a, %d %b %Y %H:%M:%S GMT").to_string()
}

fn parse_http_date(s: &str) -> Option<u64> {
    let dt = chrono::DateTime::parse_from_rfc2822(s).ok()?;
    Some(dt.timestamp() as u64)
}

fn parse_range(header: &str, file_len: u64) -> Option<(u64, u64)> {
    let h = header.trim();
    if !h.starts_with("bytes=") { return None; }
    let spec = h[6..].split(',').next()?.trim();
    let (a, b) = spec.split_once('-')?;
    if a.is_empty() {
        let n: u64 = b.parse().ok()?;
        if n == 0 || file_len == 0 { return None; }
        let n = n.min(file_len);
        return Some((file_len - n, file_len - 1));
    }
    let start: u64 = a.parse().ok()?;
    if start >= file_len { return None; }
    let end = if b.is_empty() {
        file_len - 1
    } else {
        let e: u64 = b.parse().ok()?;
        if e < start { return None; }
        e.min(file_len - 1)
    };
    Some((start, end))
}

fn extract_conditional_headers(raw: &[u8]) -> (Option<String>, Option<String>, Option<String>) {
    let mut headers = [httparse::EMPTY_HEADER; 96];
    let mut req = httparse::Request::new(&mut headers);
    if req.parse(raw).is_err() {
        return (None, None, None);
    }
    let mut inm = None;
    let mut ims = None;
    let mut rng = None;
    for h in req.headers.iter() {
        if h.name.eq_ignore_ascii_case("if-none-match") {
            inm = Some(String::from_utf8_lossy(h.value).to_string());
        } else if h.name.eq_ignore_ascii_case("if-modified-since") {
            ims = Some(String::from_utf8_lossy(h.value).to_string());
        } else if h.name.eq_ignore_ascii_case("range") {
            rng = Some(String::from_utf8_lossy(h.value).to_string());
        }
    }
    (inm, ims, rng)
}

async fn serve_static<W: AsyncWriteExt + Unpin>(
    cw: &mut W,
    path: &std::path::Path,
    req: &ParsedRequest,
    req_raw: &[u8],
) -> io::Result<(u16, u64)> {
    let is_head = req.method.eq_ignore_ascii_case("HEAD");
    if !is_head && !req.method.eq_ignore_ascii_case("GET") {
        cw.write_all(b"HTTP/1.1 405 Method Not Allowed\r\nAllow: GET, HEAD\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n").await?;
        return Ok((405, 0));
    }

    let meta = tokio::fs::metadata(path).await?;
    let file_len = meta.len();
    let mtime = meta.modified().ok();
    let mtime_secs = mtime
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let etag = format!("\"{:x}-{:x}\"", mtime_secs, file_len);
    let http_date = format_http_date(mtime);

    let (if_none_match, if_modified_since, range_header) = extract_conditional_headers(req_raw);

    if let Some(inm) = &if_none_match {
        if inm.trim() == etag || inm.trim() == "*" {
            let resp = format!(
                "HTTP/1.1 304 Not Modified\r\nETag: {etag}\r\nLast-Modified: {http_date}\r\nConnection: keep-alive\r\n\r\n"
            );
            cw.write_all(resp.as_bytes()).await?;
            return Ok((304, 0));
        }
    } else if let Some(ims) = &if_modified_since {
        if let Some(t) = parse_http_date(ims) {
            if t >= mtime_secs {
                let resp = format!(
                    "HTTP/1.1 304 Not Modified\r\nETag: {etag}\r\nLast-Modified: {http_date}\r\nConnection: keep-alive\r\n\r\n"
                );
                cw.write_all(resp.as_bytes()).await?;
                return Ok((304, 0));
            }
        }
    }

    let mime = mime_of(path);

    let (start, end, partial) = if let Some(rh) = range_header {
        match parse_range(&rh, file_len) {
            Some((s, e)) => (s, e, true),
            None => {
                let resp = format!(
                    "HTTP/1.1 416 Range Not Satisfiable\r\nContent-Range: bytes */{file_len}\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n"
                );
                cw.write_all(resp.as_bytes()).await?;
                return Ok((416, 0));
            }
        }
    } else {
        (0, file_len.saturating_sub(1), false)
    };

    let content_len: u64 = if file_len == 0 { 0 } else { end - start + 1 };

    let want_gzip = !partial
        && !is_head
        && file_len as usize >= GZIP_MIN
        && file_len as usize <= GZIP_MAX
        && is_compressible(mime)
        && client_accepts_gzip(req_raw);

    if want_gzip {
        let body = tokio::fs::read(path).await?;
        let compressed = gzip_compress(&body)?;
        if compressed.len() < body.len() {
            let head = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: {mime}\r\nContent-Encoding: gzip\r\nContent-Length: {}\r\nETag: {etag}\r\nLast-Modified: {http_date}\r\nAccept-Ranges: bytes\r\nVary: Accept-Encoding\r\nConnection: keep-alive\r\n\r\n",
                compressed.len()
            );
            cw.write_all(head.as_bytes()).await?;
            cw.write_all(&compressed).await?;
            return Ok((200, compressed.len() as u64));
        }
    }

    let status_line = if partial { "206 Partial Content" } else { "200 OK" };
    let mut head = format!(
        "HTTP/1.1 {status_line}\r\nContent-Type: {mime}\r\nContent-Length: {content_len}\r\nETag: {etag}\r\nLast-Modified: {http_date}\r\nAccept-Ranges: bytes\r\nVary: Accept-Encoding\r\n"
    );
    if partial {
        head.push_str(&format!("Content-Range: bytes {start}-{end}/{file_len}\r\n"));
    }
    head.push_str("Connection: keep-alive\r\n\r\n");
    cw.write_all(head.as_bytes()).await?;

    let mut bytes_sent: u64 = 0;
    if !is_head && content_len > 0 {
        use tokio::io::AsyncSeekExt;
        let mut file = tokio::fs::File::open(path).await?;
        file.seek(std::io::SeekFrom::Start(start)).await?;
        let mut remaining = content_len;
        let mut buf = [0u8; 32 * 1024];
        while remaining > 0 {
            let want = remaining.min(buf.len() as u64) as usize;
            let n = file.read(&mut buf[..want]).await?;
            if n == 0 { break; }
            cw.write_all(&buf[..n]).await?;
            remaining -= n as u64;
            bytes_sent += n as u64;
        }
    }

    Ok((if partial { 206 } else { 200 }, bytes_sent))
}

async fn serve_static_h2(
    respond: &mut h2::server::SendResponse<Bytes>,
    path: &std::path::Path,
    method: &str,
    req: &http::Request<h2::RecvStream>,
) -> io::Result<()> {
    let meta = tokio::fs::metadata(path).await?;
    let file_len = meta.len();
    let mtime = meta.modified().ok();
    let mtime_secs = mtime
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let etag = format!("\"{:x}-{:x}\"", mtime_secs, file_len);
    let http_date = format_http_date(mtime);

    if let Some(inm) = req.headers().get("if-none-match").and_then(|v| v.to_str().ok()) {
        if inm.trim() == etag || inm.trim() == "*" {
            let resp = Response::builder().status(304)
                .header("etag", etag)
                .header("last-modified", http_date)
                .body(())
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
            let _ = respond.send_response(resp, true)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
            return Ok(());
        }
    }

    let mime = mime_of(path);
    let is_head = method.eq_ignore_ascii_case("HEAD");

    let h2_wants_gzip = !is_head
        && (file_len as usize) >= GZIP_MIN
        && (file_len as usize) <= GZIP_MAX
        && is_compressible(mime)
        && req.headers().get("accept-encoding")
            .and_then(|v| v.to_str().ok())
            .map(|v| {
                let v = v.to_ascii_lowercase();
                v.split(',').any(|t| {
                    let t = t.trim();
                    t == "gzip" || t.starts_with("gzip;")
                })
            })
            .unwrap_or(false);

    if h2_wants_gzip {
        let body = tokio::fs::read(path).await?;
        let compressed = gzip_compress(&body)?;
        if compressed.len() < body.len() {
            let resp = Response::builder().status(200)
                .header("content-type", mime)
                .header("content-encoding", "gzip")
                .header("content-length", compressed.len())
                .header("etag", etag)
                .header("last-modified", http_date)
                .header("accept-ranges", "bytes")
                .header("vary", "accept-encoding")
                .body(())
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
            let mut send = respond.send_response(resp, false)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
            h2_send(&mut send, &compressed).await?;
            send.send_data(Bytes::new(), true)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
            return Ok(());
        }
    }

    let resp = Response::builder().status(200)
        .header("content-type", mime)
        .header("content-length", file_len)
        .header("etag", etag)
        .header("last-modified", http_date)
        .header("accept-ranges", "bytes")
        .header("vary", "accept-encoding")
        .body(())
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

    let end_stream = is_head || file_len == 0;
    let mut send = respond.send_response(resp, end_stream)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
    if end_stream { return Ok(()); }

    let mut file = tokio::fs::File::open(path).await?;
    let mut buf = [0u8; 32 * 1024];
    loop {
        let n = file.read(&mut buf).await?;
        if n == 0 { break; }
        h2_send(&mut send, &buf[..n]).await?;
    }
    send.send_data(Bytes::new(), true)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
    Ok(())
}

// ============================================================
//  GZIP helpers
// ============================================================

fn is_compressible(mime: &str) -> bool {
    let m = mime.to_ascii_lowercase();
    m.starts_with("text/")
        || m.contains("json")
        || m.contains("javascript")
        || m.contains("xml")
        || m.contains("svg")
}

fn client_accepts_gzip(raw_headers: &[u8]) -> bool {
    let mut headers = [httparse::EMPTY_HEADER; 96];
    let mut req = httparse::Request::new(&mut headers);
    if req.parse(raw_headers).is_err() { return false; }
    for h in req.headers.iter() {
        if h.name.eq_ignore_ascii_case("accept-encoding") {
            let v = String::from_utf8_lossy(h.value).to_ascii_lowercase();
            return v.split(',').any(|tok| {
                let t = tok.trim();
                t == "gzip" || t.starts_with("gzip;")
            });
        }
    }
    false
}

fn gzip_compress(data: &[u8]) -> std::io::Result<Vec<u8>> {
    use std::io::Write;
    let mut enc = flate2::write::GzEncoder::new(
        Vec::with_capacity(data.len() / 2),
        flate2::Compression::new(6),
    );
    enc.write_all(data)?;
    enc.finish()
}