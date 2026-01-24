use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use clap::Parser;
use quick_xml::events::{BytesStart, Event};
use quick_xml::{Reader, Writer};
use regex::Regex;
use serde::Deserialize;
use std::io::Cursor;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
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
}

#[derive(Clone)]
struct AppState {
    download_path: PathBuf,
    base_url: String,
    client: reqwest::Client,
}

#[derive(Deserialize)]
struct PipeQuery {
    origin: String,
}

async fn pipe_handler(
    State(state): State<Arc<AppState>>,
    Query(query): Query<PipeQuery>,
) -> Response {
    match state.client.get(&query.origin).send().await {
        Ok(response) => {
            let status = StatusCode::from_u16(response.status().as_u16())
                .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);

            if !status.is_success() {
                return (status, "Origin returned error").into_response();
            }

            match response.text().await {
                Ok(body) => match process_rss(&body, &state).await {
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
                },
                Err(e) => (
                    StatusCode::BAD_GATEWAY,
                    format!("Failed to read response body: {e}"),
                )
                    .into_response(),
            }
        }
        Err(e) => (
            StatusCode::BAD_GATEWAY,
            format!("Failed to fetch origin: {e}"),
        )
            .into_response(),
    }
}

#[allow(clippy::too_many_lines)]
async fn process_rss(xml: &str, state: &AppState) -> Result<String, Box<dyn std::error::Error>> {
    let mut reader = Reader::from_str(xml);
    let mut writer = Writer::new(Cursor::new(Vec::new()));

    let mut current_guid: Option<String> = None;
    let mut in_guid = false;
    let mut in_item = false;
    let mut item_buffer: Vec<Event<'static>> = Vec::new();
    let mut item_failed = false;

    let mut downloaded_count = 0u32;
    let mut existing_count = 0u32;
    let mut failed_count = 0u32;

    let url_regex = Regex::new(r#"url="([^"]*)""#)?;

    loop {
        match reader.read_event() {
            Ok(Event::Eof) => break,
            Ok(Event::Start(e)) => {
                let name = e.name();
                let name_str = std::str::from_utf8(name.as_ref())?;

                if name_str == "item" {
                    in_item = true;
                    item_buffer.clear();
                    item_failed = false;
                    current_guid = None;
                }

                if name_str == "guid" {
                    in_guid = true;
                }

                if in_item {
                    item_buffer.push(Event::Start(e.into_owned()));
                } else {
                    writer.write_event(Event::Start(e.clone()))?;
                }
            }
            Ok(Event::Empty(e)) => {
                let name = e.name();
                let name_str = std::str::from_utf8(name.as_ref())?;

                if name_str == "enclosure" && in_item {
                    if let Some(ref guid) = current_guid {
                        let elem_str = String::from_utf8(e.to_vec())?;

                        if let Some(caps) = url_regex.captures(&elem_str) {
                            let original_url = &caps[1];
                            let decoded_url = quick_xml::escape::unescape(original_url)?;

                            // Download the torrent if it doesn't already exist
                            let torrent_path = state.download_path.join(format!("{guid}.torrent"));
                            if torrent_path.exists() {
                                println!("Existing: {guid}");
                                existing_count += 1;
                            } else {
                                match download_file(&state.client, &decoded_url, &torrent_path)
                                    .await
                                {
                                    Ok(()) => {
                                        println!("Downloaded: {guid}");
                                        downloaded_count += 1;
                                    }
                                    Err(err) => {
                                        eprintln!("Failed: {guid} - {err}");
                                        failed_count += 1;
                                        item_failed = true;
                                        continue;
                                    }
                                }
                            }

                            // Build new enclosure with replaced URL
                            let new_url = format!(
                                "{}/{}.torrent",
                                state.base_url.trim_end_matches('/'),
                                guid
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

                            item_buffer.push(Event::Empty(new_elem.into_owned()));
                        } else {
                            // URL regex didn't match - mark item as failed
                            item_failed = true;
                        }
                    } else {
                        // No guid available - mark item as failed
                        item_failed = true;
                    }
                    continue;
                }

                if in_item {
                    item_buffer.push(Event::Empty(e.into_owned()));
                } else {
                    writer.write_event(Event::Empty(e))?;
                }
            }
            Ok(Event::End(e)) => {
                let name = e.name();
                let name_str = std::str::from_utf8(name.as_ref())?;

                if name_str == "guid" {
                    in_guid = false;
                }

                if name_str == "item" {
                    item_buffer.push(Event::End(e.into_owned()));

                    // Only write the item if download succeeded
                    if !item_failed {
                        for event in item_buffer.drain(..) {
                            writer.write_event(event)?;
                        }
                    }

                    in_item = false;
                    current_guid = None;
                } else if in_item {
                    item_buffer.push(Event::End(e.into_owned()));
                } else {
                    writer.write_event(Event::End(e))?;
                }
            }
            Ok(Event::Text(e)) => {
                if in_guid {
                    let text = e.unescape()?;
                    current_guid = Some(text.to_string());
                }
                if in_item {
                    item_buffer.push(Event::Text(e.into_owned()));
                } else {
                    writer.write_event(Event::Text(e))?;
                }
            }
            Ok(e) => {
                if in_item {
                    item_buffer.push(e.into_owned());
                } else {
                    writer.write_event(e)?;
                }
            }
            Err(e) => return Err(format!("XML parse error: {e}").into()),
        }
    }

    println!(
        "Summary: {downloaded_count} downloaded, {existing_count} existing, {failed_count} failed"
    );

    let result = writer.into_inner().into_inner();
    Ok(String::from_utf8(result)?)
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

#[tokio::main]
async fn main() {
    let args = Args::parse();

    // Create download directory if it doesn't exist
    tokio::fs::create_dir_all(&args.download_path)
        .await
        .expect("Failed to create download directory");

    let base_url = args
        .base_url
        .unwrap_or_else(|| format!("http://localhost:{}/downloaded", args.port));

    let state = Arc::new(AppState {
        download_path: args.download_path.clone(),
        base_url,
        client: reqwest::Client::new(),
    });

    let app = Router::new()
        .route("/pipe", get(pipe_handler))
        .nest_service("/downloaded", ServeDir::new(&args.download_path))
        .with_state(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], args.port));
    println!("Listening on http://{addr}");

    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
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
                        let response = b"HTTP/1.1 200 OK\r\nContent-Length: 12\r\nConnection: close\r\n\r\ntorrent data";
                        let _ = socket.write_all(response).await;
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_failed_download_removes_item() {
        let success_url = start_ok_server().await;
        let fail_url = start_fail_server(StatusCode::INTERNAL_SERVER_ERROR).await;
        let temp_dir = tempfile::tempdir().unwrap();

        let state = AppState {
            download_path: temp_dir.path().to_path_buf(),
            base_url: "http://localhost:4000/downloaded".to_string(),
            client: test_client(),
        };

        let rss = make_rss(&[
            ("item-success", &format!("{success_url}/success.torrent")),
            ("item-fail", &format!("{fail_url}/fail.torrent")),
        ]);

        let result = process_rss(&rss, &state).await.unwrap();

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
        };

        let rss = make_rss(&[("existing-item", &format!("{fail_url}/existing.torrent"))]);

        let result = process_rss(&rss, &state).await.unwrap();

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
        };

        let rss = make_rss(&[
            ("fail-item-1", &format!("{fail_url1}/fail1.torrent")),
            ("fail-item-2", &format!("{fail_url2}/fail2.torrent")),
        ]);

        let result = process_rss(&rss, &state).await.unwrap();

        // No items should be present
        assert!(!result.contains("fail-item-1"));
        assert!(!result.contains("fail-item-2"));
        // But the channel structure should remain
        assert!(result.contains("<channel>"));
        assert!(result.contains("</channel>"));
    }
}
