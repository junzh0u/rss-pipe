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

async fn process_rss(xml: &str, state: &AppState) -> Result<String, Box<dyn std::error::Error>> {
    let mut reader = Reader::from_str(xml);
    let mut writer = Writer::new(Cursor::new(Vec::new()));

    let mut current_guid: Option<String> = None;
    let mut in_guid = false;

    let url_regex = Regex::new(r#"url="([^"]*)""#)?;

    loop {
        match reader.read_event() {
            Ok(Event::Eof) => break,
            Ok(Event::Start(e)) => {
                let name = e.name();
                let name_str = std::str::from_utf8(name.as_ref())?;

                if name_str == "guid" {
                    in_guid = true;
                }

                writer.write_event(Event::Start(e.clone()))?;
            }
            Ok(Event::Empty(e)) => {
                let name = e.name();
                let name_str = std::str::from_utf8(name.as_ref())?;

                if name_str == "enclosure" {
                    if let Some(ref guid) = current_guid {
                        let elem_str = String::from_utf8(e.to_vec())?;

                        if let Some(caps) = url_regex.captures(&elem_str) {
                            let original_url = &caps[1];
                            let decoded_url = quick_xml::escape::unescape(original_url)?;

                            // Download the torrent if it doesn't already exist
                            let torrent_path = state.download_path.join(format!("{guid}.torrent"));
                            if !torrent_path.exists() {
                                if let Err(err) =
                                    download_file(&state.client, &decoded_url, &torrent_path).await
                                {
                                    eprintln!("Failed to download {decoded_url}: {err}");
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

                            writer.write_event(Event::Empty(new_elem))?;
                            continue;
                        }
                    }
                }

                writer.write_event(Event::Empty(e))?;
            }
            Ok(Event::End(e)) => {
                let name = e.name();
                let name_str = std::str::from_utf8(name.as_ref())?;

                if name_str == "guid" {
                    in_guid = false;
                } else if name_str == "item" {
                    current_guid = None;
                }

                writer.write_event(Event::End(e))?;
            }
            Ok(Event::Text(e)) => {
                if in_guid {
                    let text = e.unescape()?;
                    current_guid = Some(text.to_string());
                }
                writer.write_event(Event::Text(e))?;
            }
            Ok(e) => {
                writer.write_event(e)?;
            }
            Err(e) => return Err(format!("XML parse error: {e}").into()),
        }
    }

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
