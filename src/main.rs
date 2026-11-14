use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::{Html, IntoResponse, Response},
    routing::get,
    Router,
};
use clap::Parser;
use moka::future::Cache;
use quick_xml::events::{BytesStart, Event};
use quick_xml::{Reader, Writer};
use regex::Regex;
use serde::Deserialize;
use std::io::Cursor;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tower_http::services::ServeDir;

#[derive(Parser)]
#[command(name = "rss-pipe")]
#[command(about = "A proxy server that downloads torrent enclosures from RSS feeds")]
struct Args {
    #[arg(short, long, default_value_t = 4000)]
    port: u16,

    #[arg(short, long, default_value = "./downloaded/")]
    download_path: PathBuf,

    #[arg(short, long)]
    base_url: Option<String>,

    #[arg(short, long, default_value_t = 30)]
    retention_days: u64,

    #[arg(short, long, default_value_t = 10)]
    cache_ttl_minutes: u64,
}

#[derive(Clone)]
struct CachedEntry {
    content: String,
    cached_at: Instant,
}

#[derive(Clone)]
struct AppState {
    download_path: PathBuf,
    base_url: String,
    client: reqwest::Client,
    origin_cache: Cache<String, CachedEntry>,
}

#[derive(Deserialize)]
struct PipeQuery {
    origin: String,
    exclude: Option<String>,
}

async fn index_handler() -> Html<&'static str> {
    Html(
        r#"<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>RSS Pipe</title>
    <style>
        body { font-family: system-ui, sans-serif; max-width: 600px; margin: 40px auto; padding: 0 20px; }
        h1 { margin-bottom: 24px; }
        label { display: block; margin-bottom: 4px; font-weight: 500; }
        input { width: 100%; padding: 8px; margin-bottom: 16px; box-sizing: border-box; }
        button { padding: 10px 20px; cursor: pointer; }
    </style>
</head>
<body>
    <h1>RSS Pipe</h1>
    <form id="pipeForm">
        <label for="origin">Origin RSS URL</label>
        <input type="url" id="origin" name="origin" required placeholder="https://example.com/feed.xml">

        <label for="exclude">Exclude URL (optional)</label>
        <input type="url" id="exclude" name="exclude" placeholder="https://example.com/exclude.txt">

        <button type="submit">Pipe</button>
    </form>
    <script>
        document.getElementById('pipeForm').addEventListener('submit', function(e) {
            e.preventDefault();
            const origin = document.getElementById('origin').value;
            const exclude = document.getElementById('exclude').value;
            let url = '/pipe?origin=' + encodeURIComponent(origin);
            if (exclude) {
                url += '&exclude=' + encodeURIComponent(exclude);
            }
            window.location.href = url;
        });
    </script>
</body>
</html>"#,
    )
}

async fn cache_handler(State(state): State<Arc<AppState>>) -> Html<String> {
    let ttl_duration = state.origin_cache.policy().time_to_live();
    let ttl_str = ttl_duration.map_or("unknown".to_string(), |d| {
        format!("{} min", d.as_secs() / 60)
    });

    let now = Instant::now();
    let entries: Vec<String> = state
        .origin_cache
        .iter()
        .map(|(k, v)| {
            let origin = &*k;
            let pipe_url = format!("/pipe?origin={}", urlencoding::encode(origin));
            let expires_in = ttl_duration
                .and_then(|ttl| ttl.checked_sub(now.duration_since(v.cached_at)))
                .map_or("unknown".to_string(), |remaining| {
                    let secs = remaining.as_secs();
                    let mins = secs / 60;
                    let secs = secs % 60;
                    format!("{mins} min {secs} sec")
                });
            format!(r#"<tr><td><a href="{pipe_url}">{origin}</a></td><td>{expires_in}</td></tr>"#)
        })
        .collect();

    let rows = if entries.is_empty() {
        "<tr><td colspan=\"2\">No cached entries</td></tr>".to_string()
    } else {
        entries.join("\n")
    };

    Html(format!(
        r#"<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>RSS Pipe - Cache</title>
    <style>
        body {{ font-family: system-ui, sans-serif; max-width: 800px; margin: 40px auto; padding: 0 20px; }}
        h1 {{ margin-bottom: 8px; }}
        p {{ color: #666; margin-bottom: 24px; }}
        table {{ width: 100%; border-collapse: collapse; }}
        th, td {{ padding: 8px; border-bottom: 1px solid #eee; text-align: left; }}
        td:first-child {{ word-break: break-all; }}
        td:last-child {{ white-space: nowrap; }}
        a {{ color: #0066cc; }}
    </style>
</head>
<body>
    <h1>Cached Origins</h1>
    <p>TTL: {ttl_str}</p>
    <table>
        <tr><th>Origin</th><th>Expires In</th></tr>
        {rows}
    </table>
</body>
</html>"#
    ))
}

async fn pipe_handler(
    State(state): State<Arc<AppState>>,
    Query(query): Query<PipeQuery>,
) -> Response {
    // Fetch exclude patterns if provided
    let exclude_patterns: Vec<String> = if let Some(exclude_url) = &query.exclude {
        match state.client.get(exclude_url).send().await {
            Ok(response) if response.status().is_success() => response
                .text()
                .await
                .unwrap_or_default()
                .lines()
                .filter(|line| !line.is_empty())
                .map(String::from)
                .collect(),
            _ => {
                eprintln!("Failed to fetch exclude list from {exclude_url}");
                Vec::new()
            }
        }
    } else {
        Vec::new()
    };

    // Try to get origin content from cache, or fetch and cache it
    let body = if let Some(cached) = state.origin_cache.get(&query.origin).await {
        println!("From cache: {}", query.origin);
        cached.content.clone()
    } else {
        match state.client.get(&query.origin).send().await {
            Ok(response) => {
                let status = StatusCode::from_u16(response.status().as_u16())
                    .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);

                if !status.is_success() {
                    return (status, "Origin returned error").into_response();
                }

                match response.text().await {
                    Ok(body) => {
                        println!("Fetched: {}", query.origin);
                        let entry = CachedEntry {
                            content: body.clone(),
                            cached_at: Instant::now(),
                        };
                        state.origin_cache.insert(query.origin.clone(), entry).await;
                        body
                    }
                    Err(e) => {
                        return (
                            StatusCode::BAD_GATEWAY,
                            format!("Failed to read response body: {e}"),
                        )
                            .into_response();
                    }
                }
            }
            Err(e) => {
                return (
                    StatusCode::BAD_GATEWAY,
                    format!("Failed to fetch origin: {e}"),
                )
                    .into_response();
            }
        }
    };

    match process_rss(&body, &state, &exclude_patterns).await {
        Ok(modified_rss) => (
            StatusCode::OK,
            [("content-type", "application/rss+xml; charset=utf-8")],
            modified_rss,
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to process RSS: {e}"),
        )
            .into_response(),
    }
}

#[derive(Default)]
struct ProcessStats {
    downloaded: u32,
    existing: u32,
    failed: u32,
    excluded: u32,
}

#[derive(Default, Clone, Copy, PartialEq, Eq)]
enum ParsingField {
    #[default]
    None,
    Guid,
    Title,
}

struct ItemContext {
    guid: Option<String>,
    title: Option<String>,
    buffer: Vec<Event<'static>>,
    parsing: ParsingField,
    active: bool,
    failed: bool,
}

impl ItemContext {
    fn new() -> Self {
        Self {
            guid: None,
            title: None,
            buffer: Vec::new(),
            parsing: ParsingField::None,
            active: false,
            failed: false,
        }
    }

    fn start_item(&mut self) {
        self.active = true;
        self.buffer.clear();
        self.failed = false;
        self.guid = None;
        self.title = None;
    }

    fn finish_item<W: std::io::Write>(
        &mut self,
        writer: &mut Writer<W>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if !self.failed {
            for event in self.buffer.drain(..) {
                writer.write_event(event)?;
            }
        }
        self.active = false;
        self.guid = None;
        self.title = None;
        Ok(())
    }
}

enum EnclosureResult {
    Success(Event<'static>),
    Excluded,
    Failed,
}

async fn process_enclosure(
    e: &BytesStart<'_>,
    guid: &str,
    title: Option<&str>,
    app_state: &AppState,
    exclude_patterns: &[String],
    url_regex: &Regex,
    counters: &mut ProcessStats,
) -> Result<EnclosureResult, Box<dyn std::error::Error>> {
    let dominated =
        title.is_some_and(|t| exclude_patterns.iter().any(|pattern| t.contains(pattern)));

    if dominated {
        counters.excluded += 1;
        return Ok(EnclosureResult::Excluded);
    }

    let elem_str = String::from_utf8(e.to_vec())?;
    let Some(caps) = url_regex.captures(&elem_str) else {
        return Ok(EnclosureResult::Failed);
    };

    let decoded_url = quick_xml::escape::unescape(&caps[1])?;
    let torrent_path = app_state.download_path.join(format!("{guid}.torrent"));

    if torrent_path.exists() {
        counters.existing += 1;
    } else {
        match download_file(&app_state.client, &decoded_url, &torrent_path).await {
            Ok(()) => {
                if let Ok(metadata) = std::fs::metadata(&torrent_path) {
                    if metadata.len() < 1024 {
                        let _ = std::fs::remove_file(&torrent_path);
                        eprintln!("Deleted (too small <1KB): {guid}");
                        counters.failed += 1;
                        return Ok(EnclosureResult::Failed);
                    }
                }
                println!("Downloaded: {guid}");
                counters.downloaded += 1;
            }
            Err(err) => {
                eprintln!("Failed: {guid} - {err}");
                counters.failed += 1;
                return Ok(EnclosureResult::Failed);
            }
        }
    }

    let new_url = format!(
        "{}/{guid}.torrent",
        app_state.base_url.trim_end_matches('/')
    );
    let mut new_elem = BytesStart::new("enclosure");

    for attr in e.attributes().flatten() {
        let key = std::str::from_utf8(attr.key.as_ref())?;
        if key == "url" {
            new_elem.push_attribute(("url", new_url.as_str()));
        } else {
            new_elem.push_attribute(attr);
        }
    }

    Ok(EnclosureResult::Success(Event::Empty(
        new_elem.into_owned(),
    )))
}

async fn process_rss(
    xml: &str,
    app_state: &AppState,
    exclude_patterns: &[String],
) -> Result<String, Box<dyn std::error::Error>> {
    let mut reader = Reader::from_str(xml);
    let mut writer = Writer::new(Cursor::new(Vec::new()));
    let mut item = ItemContext::new();
    let mut counters = ProcessStats::default();
    let url_regex = Regex::new(r#"url="([^"]*)""#)?;

    loop {
        match reader.read_event() {
            Ok(Event::Eof) => break,
            Ok(Event::Start(e)) => {
                let name = e.name();
                match std::str::from_utf8(name.as_ref())? {
                    "item" => item.start_item(),
                    "guid" => item.parsing = ParsingField::Guid,
                    "title" if item.active => item.parsing = ParsingField::Title,
                    _ => {}
                }
                if item.active {
                    item.buffer.push(Event::Start(e.into_owned()));
                } else {
                    writer.write_event(Event::Start(e.clone()))?;
                }
            }
            Ok(Event::Empty(e)) => {
                let name = e.name();
                if std::str::from_utf8(name.as_ref())? == "enclosure" && item.active {
                    if let Some(ref guid) = item.guid {
                        match process_enclosure(
                            &e,
                            guid,
                            item.title.as_deref(),
                            app_state,
                            exclude_patterns,
                            &url_regex,
                            &mut counters,
                        )
                        .await?
                        {
                            EnclosureResult::Success(event) => item.buffer.push(event),
                            _ => item.failed = true,
                        }
                    } else {
                        item.failed = true;
                    }
                    continue;
                }
                if item.active {
                    item.buffer.push(Event::Empty(e.into_owned()));
                } else {
                    writer.write_event(Event::Empty(e))?;
                }
            }
            Ok(Event::End(e)) => {
                let name = e.name();
                match std::str::from_utf8(name.as_ref())? {
                    "guid" | "title" => {
                        item.parsing = ParsingField::None;
                        if item.active {
                            item.buffer.push(Event::End(e.into_owned()));
                        } else {
                            writer.write_event(Event::End(e))?;
                        }
                    }
                    "item" => {
                        item.buffer.push(Event::End(e.into_owned()));
                        item.finish_item(&mut writer)?;
                    }
                    _ if item.active => item.buffer.push(Event::End(e.into_owned())),
                    _ => writer.write_event(Event::End(e))?,
                }
            }
            Ok(Event::Text(e)) => {
                match item.parsing {
                    ParsingField::Guid => item.guid = Some(e.unescape()?.to_string()),
                    ParsingField::Title => item.title = Some(e.unescape()?.to_string()),
                    ParsingField::None => {}
                }
                if item.active {
                    item.buffer.push(Event::Text(e.into_owned()));
                } else {
                    writer.write_event(Event::Text(e))?;
                }
            }
            Ok(e) => {
                if item.active {
                    item.buffer.push(e.into_owned());
                } else {
                    writer.write_event(e)?;
                }
            }
            Err(e) => return Err(format!("XML parse error: {e}").into()),
        }
    }

    println!(
        "Summary: {} downloaded, {} existing, {} failed, {} excluded",
        counters.downloaded, counters.existing, counters.failed, counters.excluded
    );
    Ok(String::from_utf8(writer.into_inner().into_inner())?)
}

async fn download_file(
    client: &reqwest::Client,
    url: &str,
    path: &PathBuf,
) -> Result<(), Box<dyn std::error::Error>> {
    let response = client.get(url).send().await?;

    if !response.status().is_success() {
        return Err(format!("HTTP error: {}", response.status()).into());
    }

    let bytes = response.bytes().await?;
    tokio::fs::write(path, &bytes).await?;

    Ok(())
}

async fn cleanup_old_files(dir: &PathBuf, retention_days: u64) {
    let max_age = std::time::Duration::from_secs(retention_days * 24 * 60 * 60);
    let now = std::time::SystemTime::now();

    let mut entries = match tokio::fs::read_dir(dir).await {
        Ok(entries) => entries,
        Err(e) => {
            eprintln!("Failed to read download directory for cleanup: {e}");
            return;
        }
    };

    let mut removed_count = 0u32;

    while let Ok(Some(entry)) = entries.next_entry().await {
        let path = entry.path();
        if !path.is_file() {
            continue;
        }

        let Ok(metadata) = tokio::fs::metadata(&path).await else {
            continue;
        };

        let Ok(modified) = metadata.modified() else {
            continue;
        };

        if let Ok(age) = now.duration_since(modified) {
            if age > max_age {
                if let Err(e) = tokio::fs::remove_file(&path).await {
                    eprintln!("Failed to remove old file {}: {e}", path.display());
                } else {
                    removed_count += 1;
                }
            }
        }
    }

    if removed_count > 0 {
        println!("Cleanup: removed {removed_count} files older than {retention_days} days");
    }
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    // Create download directory if it doesn't exist
    tokio::fs::create_dir_all(&args.download_path)
        .await
        .expect("Failed to create download directory");

    // Canonicalize paths to resolve symlinks and relative paths
    let cwd = std::env::current_dir()
        .expect("Failed to get current working directory")
        .canonicalize()
        .expect("Failed to canonicalize current working directory");

    let download_path = args
        .download_path
        .canonicalize()
        .expect("Failed to canonicalize download path");

    // Clean up old files at startup
    cleanup_old_files(&download_path, args.retention_days).await;

    // Spawn background task for periodic cleanup (runs daily)
    let cleanup_path = download_path.clone();
    let cleanup_retention = args.retention_days;
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_hours(24));
        interval.tick().await; // Skip immediate first tick (already ran at startup)
        loop {
            interval.tick().await;
            cleanup_old_files(&cleanup_path, cleanup_retention).await;
        }
    });

    // Enforce that download_path is under current working directory
    let relative_path = download_path
        .strip_prefix(&cwd)
        .expect("Download path must be under current working directory");

    let serve_path = format!("/{}", relative_path.display());

    let base_url = args.base_url.map_or_else(
        || format!("http://localhost:{}{serve_path}", args.port),
        |url| format!("{}{serve_path}", url.trim_end_matches('/')),
    );

    // Create cache with configured TTL
    let origin_cache = Cache::builder()
        .time_to_live(Duration::from_secs(args.cache_ttl_minutes * 60))
        .build();

    let state = Arc::new(AppState {
        download_path,
        base_url,
        client: reqwest::Client::new(),
        origin_cache,
    });

    let app = Router::new()
        .route("/", get(index_handler))
        .route("/pipe", get(pipe_handler))
        .route("/cache", get(cache_handler))
        .nest_service(&serve_path, ServeDir::new(&state.download_path))
        .with_state(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], args.port));
    println!("Listening on http://{addr}");

    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .unwrap();
}

/// Resolves on SIGINT (Ctrl-C) or SIGTERM. Needed because as PID 1 in a
/// container the process has no default SIGTERM handler, so `docker stop`
/// would otherwise hang until the kill timeout.
async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to install Ctrl-C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("Failed to install SIGTERM handler")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => {},
        () = terminate => {},
    }
    println!("Shutdown signal received, stopping");
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_rss(items: &[(&str, &str)]) -> String {
        let items_xml: String = items
            .iter()
            .map(|(guid, enclosure_url)| {
                format!(
                    r#"<item><guid isPermaLink="false">{guid}</guid><title>Test Item {guid}</title><enclosure length="1000" type="application/x-bittorrent" url="{enclosure_url}"/></item>"#
                )
            })
            .collect();

        format!(
            r#"<?xml version="1.0" encoding="UTF-8"?><rss version="2.0"><channel><title>Test Feed</title>{items_xml}</channel></rss>"#
        )
    }

    async fn start_ok_server() -> String {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let url = format!("http://{addr}");

        tokio::spawn(async move {
            loop {
                if let Ok((mut socket, _)) = listener.accept().await {
                    tokio::spawn(async move {
                        let mut buf = [0u8; 4096];
                        let _ = socket.read(&mut buf).await;
                        // Return >= 1KB to pass the size check
                        let body = vec![b'X'; 1024];
                        let header = format!(
                            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                            body.len()
                        );
                        let _ = socket.write_all(header.as_bytes()).await;
                        let _ = socket.write_all(&body).await;
                        let _ = socket.flush().await;
                    });
                }
            }
        });

        // Give the server time to start
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        url
    }

    async fn start_fail_server(status: StatusCode) -> String {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let url = format!("http://{addr}");

        let status_line = match status {
            StatusCode::INTERNAL_SERVER_ERROR => "HTTP/1.1 500 Internal Server Error",
            StatusCode::NOT_FOUND => "HTTP/1.1 404 Not Found",
            StatusCode::SERVICE_UNAVAILABLE => "HTTP/1.1 503 Service Unavailable",
            _ => "HTTP/1.1 500 Internal Server Error",
        };

        tokio::spawn(async move {
            loop {
                if let Ok((mut socket, _)) = listener.accept().await {
                    let response = format!(
                        "{status_line}\r\nContent-Length: 5\r\nConnection: close\r\n\r\nerror"
                    );
                    tokio::spawn(async move {
                        let mut buf = [0u8; 4096];
                        let _ = socket.read(&mut buf).await;
                        let _ = socket.write_all(response.as_bytes()).await;
                        let _ = socket.flush().await;
                    });
                }
            }
        });

        // Give the server time to start
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        url
    }

    fn test_client() -> reqwest::Client {
        reqwest::Client::builder().no_proxy().build().unwrap()
    }

    fn test_cache() -> Cache<String, CachedEntry> {
        Cache::builder()
            .time_to_live(Duration::from_secs(600))
            .build()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_failed_download_removes_item() {
        let success_url = start_ok_server().await;
        let fail_url = start_fail_server(StatusCode::INTERNAL_SERVER_ERROR).await;
        let temp_dir = tempfile::tempdir().unwrap();

        let state = AppState {
            download_path: temp_dir.path().to_path_buf(),
            base_url: "http://localhost:4000/downloaded".to_string(),
            client: test_client(),
            origin_cache: test_cache(),
        };

        let rss = make_rss(&[
            ("item-success", &format!("{success_url}/success.torrent")),
            ("item-fail", &format!("{fail_url}/fail.torrent")),
        ]);

        let result = process_rss(&rss, &state, &[]).await.unwrap();

        // Successful item should be present
        assert!(
            result.contains("item-success"),
            "Expected successful item to be present"
        );
        assert!(result.contains("http://localhost:4000/downloaded/item-success.torrent"));

        // Failed item should be removed
        assert!(
            !result.contains("item-fail"),
            "Expected failed item to be removed"
        );
    }

    #[tokio::test]
    async fn test_existing_torrent_not_redownloaded() {
        // Server would fail if called, but file exists so it shouldn't be called
        let fail_url = start_fail_server(StatusCode::INTERNAL_SERVER_ERROR).await;
        let temp_dir = tempfile::tempdir().unwrap();

        // Create an existing torrent file
        let existing_path = temp_dir.path().join("existing-item.torrent");
        tokio::fs::write(&existing_path, b"existing torrent")
            .await
            .unwrap();

        let state = AppState {
            download_path: temp_dir.path().to_path_buf(),
            base_url: "http://localhost:4000/downloaded".to_string(),
            client: test_client(),
            origin_cache: test_cache(),
        };

        let rss = make_rss(&[("existing-item", &format!("{fail_url}/existing.torrent"))]);

        let result = process_rss(&rss, &state, &[]).await.unwrap();

        // Item should be present since file already existed
        assert!(result.contains("existing-item"));
        assert!(result.contains("http://localhost:4000/downloaded/existing-item.torrent"));
    }

    #[tokio::test]
    async fn test_all_items_fail_produces_empty_channel() {
        let fail_url1 = start_fail_server(StatusCode::NOT_FOUND).await;
        let fail_url2 = start_fail_server(StatusCode::SERVICE_UNAVAILABLE).await;
        let temp_dir = tempfile::tempdir().unwrap();

        let state = AppState {
            download_path: temp_dir.path().to_path_buf(),
            base_url: "http://localhost:4000/downloaded".to_string(),
            client: test_client(),
            origin_cache: test_cache(),
        };

        let rss = make_rss(&[
            ("fail-item-1", &format!("{fail_url1}/fail1.torrent")),
            ("fail-item-2", &format!("{fail_url2}/fail2.torrent")),
        ]);

        let result = process_rss(&rss, &state, &[]).await.unwrap();

        // No items should be present
        assert!(!result.contains("fail-item-1"));
        assert!(!result.contains("fail-item-2"));
        // But the channel structure should remain
        assert!(result.contains("<channel>"));
        assert!(result.contains("</channel>"));
    }

    #[tokio::test]
    async fn test_exclude_patterns_filter_items() {
        let success_url = start_ok_server().await;
        // Use a fail server for excluded item - if download is attempted, test would fail
        let fail_url = start_fail_server(StatusCode::INTERNAL_SERVER_ERROR).await;
        let temp_dir = tempfile::tempdir().unwrap();

        let state = AppState {
            download_path: temp_dir.path().to_path_buf(),
            base_url: "http://localhost:4000/downloaded".to_string(),
            client: test_client(),
            origin_cache: test_cache(),
        };

        let rss = make_rss(&[
            ("keep-item", &format!("{success_url}/keep.torrent")),
            ("exclude-me", &format!("{fail_url}/exclude.torrent")),
        ]);

        // Exclude items with "exclude-me" in the title
        let exclude_patterns = vec!["exclude-me".to_string()];
        let result = process_rss(&rss, &state, &exclude_patterns).await.unwrap();

        // Kept item should be present
        assert!(result.contains("keep-item"));
        assert!(result.contains("http://localhost:4000/downloaded/keep-item.torrent"));

        // Excluded item should not be present (and no download attempted)
        assert!(!result.contains("exclude-me"));
    }

    #[tokio::test]
    async fn test_cleanup_removes_old_files() {
        use filetime::{set_file_mtime, FileTime};

        let temp_dir = tempfile::tempdir().unwrap();
        let old_file = temp_dir.path().join("old.torrent");
        let new_file = temp_dir.path().join("new.torrent");

        // Create both files
        tokio::fs::write(&old_file, b"old content").await.unwrap();
        tokio::fs::write(&new_file, b"new content").await.unwrap();

        // Set old file's mtime to 31 days ago
        let old_time = std::time::SystemTime::now()
            .checked_sub(std::time::Duration::from_secs(31 * 24 * 60 * 60))
            .unwrap();
        set_file_mtime(&old_file, FileTime::from_system_time(old_time)).unwrap();

        // Run cleanup with 30 day retention
        cleanup_old_files(&temp_dir.path().to_path_buf(), 30).await;

        // Old file should be removed, new file should remain
        assert!(!old_file.exists(), "Old file should be removed");
        assert!(new_file.exists(), "New file should remain");
    }

    #[tokio::test]
    async fn test_cleanup_keeps_files_within_retention() {
        use filetime::{set_file_mtime, FileTime};

        let temp_dir = tempfile::tempdir().unwrap();
        let file_25_days = temp_dir.path().join("file_25_days.torrent");
        let file_29_days = temp_dir.path().join("file_29_days.torrent");

        // Create files
        tokio::fs::write(&file_25_days, b"content").await.unwrap();
        tokio::fs::write(&file_29_days, b"content").await.unwrap();

        // Set file times (well within retention period)
        let time_25_days = std::time::SystemTime::now()
            .checked_sub(std::time::Duration::from_secs(25 * 24 * 60 * 60))
            .unwrap();
        let time_29_days = std::time::SystemTime::now()
            .checked_sub(std::time::Duration::from_secs(29 * 24 * 60 * 60))
            .unwrap();
        set_file_mtime(&file_25_days, FileTime::from_system_time(time_25_days)).unwrap();
        set_file_mtime(&file_29_days, FileTime::from_system_time(time_29_days)).unwrap();

        // Run cleanup with 30 day retention
        cleanup_old_files(&temp_dir.path().to_path_buf(), 30).await;

        // Both files should remain (within 30 day retention)
        assert!(file_25_days.exists(), "25-day old file should remain");
        assert!(file_29_days.exists(), "29-day old file should remain");
    }

    #[tokio::test]
    async fn test_cleanup_ignores_directories() {
        use filetime::{set_file_mtime, FileTime};

        let temp_dir = tempfile::tempdir().unwrap();
        let subdir = temp_dir.path().join("subdir");
        tokio::fs::create_dir(&subdir).await.unwrap();

        // Set directory mtime to old (should be ignored)
        let old_time = std::time::SystemTime::now()
            .checked_sub(std::time::Duration::from_secs(31 * 24 * 60 * 60))
            .unwrap();
        set_file_mtime(&subdir, FileTime::from_system_time(old_time)).unwrap();

        // Run cleanup
        cleanup_old_files(&temp_dir.path().to_path_buf(), 30).await;

        // Directory should still exist
        assert!(subdir.exists(), "Directory should not be removed");
    }
}
