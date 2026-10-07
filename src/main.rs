//! banditx v0.0.6 — HTTP/1.1 + HTTP/2 + FastCGI reverse proxy.
//!
//! Новое vs v0.0.5:
//!  - kind: fastcgi в маршрутах (плюс doc_root)
//!  - Полный FastCGI-клиент: BEGIN_REQUEST, PARAMS, STDIN, чтение STDOUT/END_REQUEST
//!  - Парсинг CGI-ответа и превращение в HTTP/1.1

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use h2::server as h2_server;
use http::{HeaderValue, Response};
use serde::Deserialize;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;
use tokio_rustls::TlsAcceptor;

const MAX_HEADER_BYTES: usize = 64 * 1024;
const HEADER_TIMEOUT: Duration = Duration::from_secs(15);
const BODY_TIMEOUT: Duration = Duration::from_secs(120);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_FCGI_BODY: usize = 16 * 1024 * 1024;

// ============================================================
//  КОНФИГ
// ============================================================

#[derive(Debug, Deserialize)]
struct Config {
    server: ServerCfg,
    routes: Vec<RouteCfg>,
}

#[derive(Debug, Deserialize)]
struct ServerCfg {
    listen: String,
    #[serde(default)]
    tls_listen: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RouteCfg {
    prefix: String,
    upstream: String,
    #[serde(default)]
    kind: RouteKind,
    #[serde(default)]
    doc_root: Option<String>,
}

#[derive(Debug, Clone, Copy, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
enum RouteKind {
    #[default]
    Http,
    Fastcgi,
}

impl Config {
    fn pick(&self, path: &str) -> Option<&RouteCfg> {
        self.routes
            .iter()
            .filter(|r| path.starts_with(&r.prefix))
            .max_by_key(|r| r.prefix.len())
    }
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
        "localhost".to_string(),
        "127.0.0.1".to_string(),
    ])
    .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("cert gen: {e}")))?;

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
    let cfg_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "banditx.yaml".to_string());

    let text = std::fs::read_to_string(&cfg_path)?;
    let cfg: Config = serde_yaml::from_str(&text)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("config: {e}")))?;

    if cfg.routes.is_empty() {
        eprintln!("no routes configured");
        std::process::exit(1);
    }

    let cfg = Arc::new(cfg);

    let listener = TcpListener::bind(&cfg.server.listen).await?;
    println!(
        "banditx v0.0.6 | http://{} | {} route(s)",
        cfg.server.listen,
        cfg.routes.len()
    );

    if let Some(tls_addr) = &cfg.server.tls_listen {
        let acceptor = build_tls_acceptor()?;
        let listener_tls = TcpListener::bind(tls_addr).await?;
        println!("                https://{tls_addr} (self-signed, h2 + http/1.1)");

        let cfg_tls = Arc::clone(&cfg);
        tokio::spawn(async move {
            loop {
                let (sock, peer) = match listener_tls.accept().await {
                    Ok(x) => x,
                    Err(e) => {
                        eprintln!("tls accept: {e}");
                        continue;
                    }
                };
                let acceptor = acceptor.clone();
                let cfg = Arc::clone(&cfg_tls);
                tokio::spawn(async move {
                    let tls_stream = match acceptor.accept(sock).await {
                        Ok(s) => s,
                        Err(e) => {
                            eprintln!("[tls {peer}] {e}");
                            return;
                        }
                    };
                    let alpn = tls_stream.get_ref().1.alpn_protocol().map(|p| p.to_vec());
                    match alpn.as_deref() {
                        Some(b"h2") => {
                            println!("[{peer}] ALPN: h2");
                            if let Err(e) = serve_h2(tls_stream, peer, cfg).await {
                                eprintln!("[h2 {peer}] {e}");
                            }
                        }
                        _ => {
                            println!("[{peer}] ALPN: http/1.1");
                            if let Err(e) = serve_h1(tls_stream, peer, cfg).await {
                                eprintln!("[h1-tls {peer}] {e}");
                            }
                        }
                    }
                });
            }
        });
    }

    for r in &cfg.routes {
        let k = match r.kind { RouteKind::Http => "http", RouteKind::Fastcgi => "fcgi" };
        println!("  {} -> {} [{}]", r.prefix, r.upstream, k);
    }

    loop {
        let (client, peer) = listener.accept().await?;
        let cfg = Arc::clone(&cfg);
        tokio::spawn(async move {
            if let Err(e) = serve_h1(client, peer, cfg).await {
                eprintln!("[{peer}] {e}");
            }
        });
    }
}

// ============================================================
//  HTTP/1.1 KEEP-ALIVE LOOP
// ============================================================

async fn serve_h1<S>(stream: S, peer: SocketAddr, cfg: Arc<Config>) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (r, mut cw) = tokio::io::split(stream);
    let mut cr = BufReader::new(r);

    loop {
        let req_raw = match read_header_block(&mut cr).await? {
            Some(b) => b,
            None => return Ok(()),
        };

        let parsed = match parse_request(&req_raw) {
            Ok(p) => p,
            Err(e) => {
                cw.write_all(b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").await.ok();
                return Err(e);
            }
        };

        let route = match cfg.pick(&parsed.path) {
            Some(r) => r,
            None => {
                cw.write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").await.ok();
                return Ok(());
            }
        };

        let keep = match route.kind {
            RouteKind::Http => {
                match proxy_http_h1(&mut cr, &mut cw, &parsed, &req_raw, peer, &route.upstream).await {
                    Ok(k) => k,
                    Err(e) => {
                        eprintln!("[{peer}] http: {e}");
                        false
                    }
                }
            }
            RouteKind::Fastcgi => {
                match proxy_fastcgi_h1(&mut cr, &mut cw, &parsed, &req_raw, peer, route).await {
                    Ok(k) => k,
                    Err(e) => {
                        eprintln!("[{peer}] fcgi: {e}");
                        false
                    }
                }
            }
        };

        if !keep || parsed.connection_close {
            return Ok(());
        }
    }
}

// ============================================================
//  HTTP PROXY (как в v0.0.5)
// ============================================================

async fn proxy_http_h1<R, W>(
    cr: &mut R, cw: &mut W,
    parsed: &ParsedRequest, req_raw: &[u8],
    peer: SocketAddr, upstream: &str,
) -> io::Result<bool>
where
    R: AsyncBufReadExt + Unpin,
    W: AsyncWriteExt + Unpin,
{
    let up = match timeout(CONNECT_TIMEOUT, TcpStream::connect(upstream)).await {
        Ok(Ok(s)) => s,
        _ => {
            cw.write_all(b"HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").await.ok();
            return Ok(false);
        }
    };
    up.set_nodelay(true).ok();
    let (ur, mut uw) = up.into_split();
    let mut ur = BufReader::new(ur);

    let fwd = build_forward_request(parsed, peer, req_raw);
    uw.write_all(&fwd).await?;

    if forward_body(cr, &mut uw, parsed.framing).await.is_err() {
        cw.write_all(b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").await.ok();
        return Ok(false);
    }
    uw.flush().await.ok();

    let resp_raw = match read_header_block(&mut ur).await? {
        Some(b) => b,
        None => {
            cw.write_all(b"HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").await.ok();
            return Ok(false);
        }
    };

    let resp = match parse_response(&resp_raw) {
        Ok(r) => r,
        Err(_) => {
            cw.write_all(b"HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").await.ok();
            return Ok(false);
        }
    };

    let upstream_close = resp.connection_close;
    let framing = response_framing(&resp, &parsed.method);
    let close_client = parsed.connection_close
        || upstream_close
        || matches!(framing, Framing::UntilClose);

    let out_head = build_response_head(&resp, close_client);
    cw.write_all(&out_head).await?;
    forward_response_body(&mut ur, cw, framing).await?;

    Ok(!close_client)
}

// ============================================================
//  FastCGI PROXY
// ============================================================

const FCGI_VERSION_1: u8 = 1;
const FCGI_BEGIN_REQUEST: u8 = 1;
const FCGI_END_REQUEST: u8 = 3;
const FCGI_PARAMS: u8 = 4;
const FCGI_STDIN: u8 = 5;
const FCGI_STDOUT: u8 = 6;
const FCGI_STDERR: u8 = 7;
const FCGI_RESPONDER: u16 = 1;

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
        if n < 128 {
            out.push(n as u8);
        } else {
            out.extend_from_slice(&((n as u32) | 0x8000_0000).to_be_bytes());
        }
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
    if clen > 0 {
        r.read_exact(&mut content).await?;
    }
    if pad > 0 {
        let mut p = vec![0u8; pad];
        r.read_exact(&mut p).await?;
    }
    Ok(Some((ty, req_id, content)))
}

async fn proxy_fastcgi_h1<R, W>(
    cr: &mut R, cw: &mut W,
    parsed: &ParsedRequest, req_raw: &[u8],
    peer: SocketAddr, route: &RouteCfg,
) -> io::Result<bool>
where
    R: AsyncBufReadExt + Unpin,
    W: AsyncWriteExt + Unpin,
{
    // 1. Читаем тело в буфер (FPM ждёт CONTENT_LENGTH)
    let body = read_body_bounded(cr, parsed.framing, MAX_FCGI_BODY).await?;

    // 2. Коннект
    let up = match timeout(CONNECT_TIMEOUT, TcpStream::connect(&route.upstream)).await {
        Ok(Ok(s)) => s,
        _ => {
            cw.write_all(b"HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").await.ok();
            return Ok(false);
        }
    };
    up.set_nodelay(true).ok();
    let (ur, mut uw) = up.into_split();
    let mut ur = BufReader::new(ur);

    // 3. PARAMS
    let doc_root = route.doc_root.as_deref().unwrap_or("/");
    let script_filename = format!(
        "{}{}",
        doc_root.trim_end_matches('/'),
        parsed.path
    );

    let mut params = Vec::new();
    let add = |k: &str, v: &str, out: &mut Vec<u8>| {
        fcgi_encode_nv(out, k.as_bytes(), v.as_bytes())
    };

    add("GATEWAY_INTERFACE", "CGI/1.1", &mut params);
    add("SERVER_SOFTWARE", "banditx/0.0.6", &mut params);
    add("SERVER_PROTOCOL", "HTTP/1.1", &mut params);
    add("REQUEST_METHOD", &parsed.method, &mut params);
    let uri = if parsed.query.is_empty() {
        parsed.path.clone()
    } else {
        format!("{}?{}", parsed.path, parsed.query)
    };
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

    // HTTP_* из оригинальных заголовков
    {
        let mut hdrs = [httparse::EMPTY_HEADER; 96];
        let mut hreq = httparse::Request::new(&mut hdrs);
        if hreq.parse(req_raw).is_ok() {
            for h in hreq.headers.iter() {
                let n = h.name.to_ascii_uppercase().replace('-', "_");
                let v = std::str::from_utf8(h.value).unwrap_or("");
                if n == "CONTENT_TYPE" {
                    add("CONTENT_TYPE", v, &mut params);
                } else if n != "CONTENT_LENGTH" {
                    add(&format!("HTTP_{n}"), v, &mut params);
                }
            }
        }
    }

    // 4. BEGIN_REQUEST
    let req_id: u16 = 1;
    let mut begin = Vec::with_capacity(8);
    begin.extend_from_slice(&FCGI_RESPONDER.to_be_bytes());
    begin.push(0);
    begin.extend_from_slice(&[0u8; 5]);

    let mut out = Vec::new();
    fcgi_write_record(&mut out, req_id, FCGI_BEGIN_REQUEST, &begin);
    uw.write_all(&out).await?;

    // 5. PARAMS (чанками по 65535)
    for chunk in params.chunks(65535) {
        let mut b = Vec::new();
        fcgi_write_record(&mut b, req_id, FCGI_PARAMS, chunk);
        uw.write_all(&b).await?;
    }
    let mut b = Vec::new();
    fcgi_write_record(&mut b, req_id, FCGI_PARAMS, &[]);
    uw.write_all(&b).await?;

    // 6. STDIN
    for chunk in body.chunks(65535) {
        let mut b = Vec::new();
        fcgi_write_record(&mut b, req_id, FCGI_STDIN, chunk);
        uw.write_all(&b).await?;
    }
    let mut b = Vec::new();
    fcgi_write_record(&mut b, req_id, FCGI_STDIN, &[]);
    uw.write_all(&b).await?;
    uw.flush().await.ok();

    // 7. Читаем STDOUT/STDERR до END_REQUEST
    let mut stdout = Vec::new();
    loop {
        let rec = match timeout(BODY_TIMEOUT, fcgi_read_record(&mut ur)).await {
            Ok(Ok(x)) => x,
            Ok(Err(e)) => {
                eprintln!("[fcgi read err] {e}");
                break;
            }
            Err(_) => {
                eprintln!("[fcgi timeout]");
                break;
            }
        };
        let Some((ty, _id, content)) = rec else { break };
        match ty {
            FCGI_STDOUT => stdout.extend_from_slice(&content),
            FCGI_STDERR => {
                if !content.is_empty() {
                    eprintln!("[fcgi stderr] {}", String::from_utf8_lossy(&content));
                }
            }
            FCGI_END_REQUEST => break,
            _ => {}
        }
    }

    // 8. Превращаем CGI-ответ в HTTP/1.1
    let (cgi_headers, cgi_body) = split_cgi_response(&stdout);

    let mut status_line = "200 OK".to_string();
    let mut http_headers: Vec<(String, String)> = Vec::new();
    for (n, v) in cgi_headers {
        if n.eq_ignore_ascii_case("status") {
            status_line = v;
            continue;
        }
        if n.eq_ignore_ascii_case("content-length") {
            continue;   // свой поставим
        }
        if is_hop_by_hop(&n) { continue; }
        http_headers.push((n, v));
    }
    http_headers.push(("Content-Length".to_string(), cgi_body.len().to_string()));

    let mut out = format!("HTTP/1.1 {}\r\n", status_line);
    for (n, v) in &http_headers {
        out.push_str(n);
        out.push_str(": ");
        out.push_str(v);
        out.push_str("\r\n");
    }
    if parsed.connection_close {
        out.push_str("Connection: close\r\n");
    } else {
        out.push_str("Connection: keep-alive\r\n");
    }
    out.push_str("\r\n");
    cw.write_all(out.as_bytes()).await?;
    cw.write_all(&cgi_body).await?;

    Ok(!parsed.connection_close)
}

fn split_cgi_response(raw: &[u8]) -> (Vec<(String, String)>, Vec<u8>) {
    let idx = find_subslice(raw, b"\r\n\r\n")
        .map(|i| (i, 4))
        .or_else(|| find_subslice(raw, b"\n\n").map(|i| (i, 2)));
    let Some((i, sep)) = idx else {
        return (Vec::new(), raw.to_vec());
    };
    let header_block = &raw[..i];
    let body = raw[i + sep..].to_vec();
    let mut headers = Vec::new();
    for line in header_block.split(|&b| b == b'\n') {
        let line = line.strip_suffix(b"\r").unwrap_or(line);
        if line.is_empty() { continue; }
        if let Some(colon) = line.iter().position(|&b| b == b':') {
            let name = String::from_utf8_lossy(&line[..colon]).trim().to_string();
            let value = String::from_utf8_lossy(&line[colon + 1..]).trim().to_string();
            headers.push((name, value));
        }
    }
    (headers, body)
}

fn find_subslice(hay: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || hay.len() < needle.len() {
        return None;
    }
    hay.windows(needle.len()).position(|w| w == needle)
}

// ============================================================
//  HTTP/2 SERVER
// ============================================================

async fn serve_h2<S>(stream: S, peer: SocketAddr, cfg: Arc<Config>) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let mut conn = h2_server::Builder::new()
        .max_concurrent_streams(256)
        .initial_window_size(1024 * 1024)
        .initial_connection_window_size(16 * 1024 * 1024)
        .handshake(stream)
        .await
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("h2: {e}")))?;

    while let Some(r) = conn.accept().await {
        let (req, respond) = match r {
            Ok(x) => x,
            Err(e) => {
                eprintln!("[{peer}] h2 accept: {e}");
                break;
            }
        };
        let cfg = Arc::clone(&cfg);
        tokio::spawn(async move {
            if let Err(e) = handle_h2_stream(req, respond, peer, cfg).await {
                eprintln!("[{peer}] h2 stream: {e}");
            }
        });
    }
    Ok(())
}

async fn handle_h2_stream(
    req: http::Request<h2::RecvStream>,
    mut respond: h2::server::SendResponse<Bytes>,
    peer: SocketAddr,
    cfg: Arc<Config>,
) -> io::Result<()> {
    let method = req.method().to_string();
    let path = req.uri().path().to_string();
    let query = req.uri().query().unwrap_or("");
    let target = if query.is_empty() {
        path.clone()
    } else {
        format!("{path}?{query}")
    };

    let route = match cfg.pick(&path) {
        Some(r) => r,
        None => return send_h2_simple(&mut respond, 404, b"not found").await,
    };

    if route.kind == RouteKind::Fastcgi {
        return send_h2_simple(&mut respond, 501, b"fastcgi over h2 not implemented yet").await;
    }

    let up = match timeout(CONNECT_TIMEOUT, TcpStream::connect(&route.upstream)).await {
        Ok(Ok(s)) => s,
        _ => return send_h2_simple(&mut respond, 502, b"upstream down").await,
    };
    up.set_nodelay(true).ok();
    let (ur, mut uw) = up.into_split();
    let mut ur = BufReader::new(ur);

    let mut fwd = Vec::with_capacity(512);
    fwd.extend_from_slice(method.as_bytes());
    fwd.push(b' ');
    fwd.extend_from_slice(target.as_bytes());
    fwd.extend_from_slice(b" HTTP/1.1\r\n");

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
    let authority = req.uri().authority().map(|a| a.to_string())
        .unwrap_or_else(|| route.upstream.clone());
    fwd.extend_from_slice(b"Host: ");
    fwd.extend_from_slice(authority.as_bytes());
    fwd.extend_from_slice(b"\r\n");
    fwd.extend_from_slice(b"X-Forwarded-For: ");
    fwd.extend_from_slice(peer.ip().to_string().as_bytes());
    fwd.extend_from_slice(b"\r\nX-Forwarded-Proto: https\r\nConnection: keep-alive\r\n\r\n");
    uw.write_all(&fwd).await?;

    let mut body = req.into_body();
    while let Some(chunk) = body.data().await {
        let chunk = chunk.map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        uw.write_all(&chunk).await?;
    }
    uw.flush().await.ok();

    let resp_raw = match read_header_block(&mut ur).await? {
        Some(b) => b,
        None => return send_h2_simple(&mut respond, 502, b"no response").await,
    };
    let resp = parse_response(&resp_raw)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "bad response"))?;
    let framing = response_framing(&resp, &method);

    let mut h2_resp = Response::builder()
        .status(resp.code)
        .body(())
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
    {
        let hdrs = h2_resp.headers_mut();
        for (n, v) in &resp.headers {
            if is_hop_by_hop(n) { continue; }
            if n.eq_ignore_ascii_case("transfer-encoding") { continue; }
            if let (Ok(name), Ok(val)) = (
                http::header::HeaderName::from_bytes(n.as_bytes()),
                HeaderValue::from_str(v),
            ) {
                hdrs.append(name, val);
            }
        }
    }
    let mut send = respond
        .send_response(h2_resp, false)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

    stream_h1_body_to_h2(&mut ur, &mut send, framing).await?;
    send.send_data(Bytes::new(), true)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
    Ok(())
}

async fn send_h2_simple(
    respond: &mut h2::server::SendResponse<Bytes>,
    status: u16,
    body: &[u8],
) -> io::Result<()> {
    let resp = Response::builder()
        .status(status)
        .header("content-type", "text/plain; charset=utf-8")
        .header("content-length", body.len())
        .body(())
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
    let mut send = respond
        .send_response(resp, body.is_empty())
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
    if !body.is_empty() {
        send.send_data(Bytes::copy_from_slice(body), true)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
    }
    Ok(())
}

async fn stream_h1_body_to_h2<R>(
    r: &mut R,
    send: &mut h2::SendStream<Bytes>,
    framing: Framing,
) -> io::Result<()>
where
    R: AsyncBufReadExt + Unpin,
{
    let mut buf = [0u8; 16 * 1024];
    match framing {
        Framing::None => Ok(()),
        Framing::ContentLength(n) => {
            let mut rem = n;
            while rem > 0 {
                let want = rem.min(buf.len());
                let got = r.read(&mut buf[..want]).await?;
                if got == 0 {
                    return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "eof"));
                }
                h2_send(send, &buf[..got]).await?;
                rem -= got;
            }
            Ok(())
        }
        Framing::Chunked => loop {
            let mut line = Vec::new();
            let n = r.read_until(b'\n', &mut line).await?;
            if n == 0 { return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "eof chunk")); }
            let s = std::str::from_utf8(&line)
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "chunk hdr"))?;
            let hex = s.split(';').next().unwrap_or("").trim();
            let size = usize::from_str_radix(hex, 16)
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "chunk size"))?;
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
//  PARSE / FRAMING / STREAM
// ============================================================

struct ParsedRequest {
    method: String,
    path: String,
    query: String,
    framing: Framing,
    connection_close: bool,
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

    let mut cl: Option<usize> = None;
    let mut chunked = false;
    let mut connection_close = false;

    for h in req.headers.iter() {
        let n = h.name;
        let v = std::str::from_utf8(h.value).unwrap_or("");
        if n.eq_ignore_ascii_case("content-length") {
            cl = v.trim().parse().ok();
        } else if n.eq_ignore_ascii_case("transfer-encoding") {
            if v.to_ascii_lowercase().contains("chunked") { chunked = true; }
        } else if n.eq_ignore_ascii_case("connection") {
            if v.to_ascii_lowercase().contains("close") { connection_close = true; }
        }
    }
    if chunked && cl.is_some() {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "CL+TE"));
    }
    let framing = if chunked { Framing::Chunked } else {
        match cl { Some(0) | None => Framing::None, Some(n) => Framing::ContentLength(n) }
    };
    Ok(ParsedRequest { method, path, query, framing, connection_close })
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
    if resp.code == 204 || resp.code == 304 || (100..200).contains(&resp.code) {
        return Framing::None;
    }
    let mut cl: Option<usize> = None;
    let mut chunked = false;
    for (n, v) in &resp.headers {
        if n.eq_ignore_ascii_case("content-length") {
            cl = v.trim().parse().ok();
        } else if n.eq_ignore_ascii_case("transfer-encoding")
            && v.to_ascii_lowercase().contains("chunked") {
            chunked = true;
        }
    }
    if chunked { Framing::Chunked } else {
        match cl {
            Some(0) => Framing::None,
            Some(n) => Framing::ContentLength(n),
            None => Framing::UntilClose,
        }
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
    for h in hreq.headers.iter() {
        let n = h.name;
        let v = h.value;
        let ln = n.to_ascii_lowercase();
        if matches!(ln.as_str(),
            "connection" | "keep-alive" | "proxy-authenticate" | "proxy-authorization"
            | "proxy-connection" | "te" | "trailer" | "upgrade") {
            continue;
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
    out.extend_from_slice(b"X-Forwarded-Proto: http\r\nConnection: keep-alive\r\n\r\n");
    out
}

fn build_response_head(resp: &ParsedResponse, close: bool) -> Vec<u8> {
    let mut out = Vec::with_capacity(256);
    out.extend_from_slice(format!("HTTP/1.1 {} {}\r\n", resp.code, resp.reason).as_bytes());
    for (n, v) in &resp.headers {
        let ln = n.to_ascii_lowercase();
        if matches!(ln.as_str(),
            "connection" | "keep-alive" | "proxy-authenticate" | "proxy-authorization"
            | "proxy-connection" | "te" | "trailer" | "upgrade") {
            continue;
        }
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
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "eof hdrs"));
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
            if n == 0 { return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "eof chunk")); }
            w.write_all(&line).await?;
            let s = std::str::from_utf8(&line).map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "chunk hdr"))?;
            let hex = s.split(';').next().unwrap_or("").trim();
            let size = usize::from_str_radix(hex, 16).map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "size"))?;
            if size == 0 {
                loop {
                    let mut t = Vec::new();
                    let n = r.read_until(b'\n', &mut t).await?;
                    if n == 0 { return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "eof tr")); }
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
            if n > max { return Err(io::Error::new(io::ErrorKind::InvalidData, "body too large")); }
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
            if n == 0 { return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "eof chunk")); }
            let s = std::str::from_utf8(&line).map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "chunk hdr"))?;
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
            if out.len() + size > max {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "body too large"));
            }
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

async fn forward_response_body<R, W>(r: &mut R, w: &mut W, framing: Framing) -> io::Result<()>
where R: AsyncBufReadExt + Unpin, W: AsyncWriteExt + Unpin {
    let mut buf = [0u8; 16 * 1024];
    match framing {
        Framing::None => Ok(()),
        Framing::ContentLength(n) => {
            let mut rem = n;
            while rem > 0 {
                let want = rem.min(buf.len());
                let got = r.read(&mut buf[..want]).await?;
                if got == 0 { return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "eof")); }
                w.write_all(&buf[..got]).await?;
                rem -= got;
            }
            Ok(())
        }
        Framing::Chunked => loop {
            let mut line = Vec::new();
            let n = r.read_until(b'\n', &mut line).await?;
            if n == 0 { return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "eof chunk")); }
            w.write_all(&line).await?;
            let s = std::str::from_utf8(&line).map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "chunk hdr"))?;
            let hex = s.split(';').next().unwrap_or("").trim();
            let size = usize::from_str_radix(hex, 16).map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "size"))?;
            if size == 0 {
                loop {
                    let mut t = Vec::new();
                    let n = r.read_until(b'\n', &mut t).await?;
                    if n == 0 { return Ok(()); }
                    w.write_all(&t).await?;
                    if t == b"\r\n" || t == b"\n" { return Ok(()); }
                }
            }
            let mut rem = size;
            while rem > 0 {
                let want = rem.min(buf.len());
                let got = r.read(&mut buf[..want]).await?;
                if got == 0 { return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "eof")); }
                w.write_all(&buf[..got]).await?;
                rem -= got;
            }
            let mut crlf = [0u8; 2];
            r.read_exact(&mut crlf).await?;
            w.write_all(&crlf).await?;
        },
        Framing::UntilClose => loop {
            let n = r.read(&mut buf).await?;
            if n == 0 { return Ok(()); }
            w.write_all(&buf[..n]).await?;
        },
    }
}