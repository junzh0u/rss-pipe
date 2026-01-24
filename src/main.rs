use axum::{
    extract::Query,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use clap::Parser;
use serde::Deserialize;
use std::net::SocketAddr;

#[derive(Parser)]
#[command(name = "rss-pipe")]
#[command(about = "A simple proxy server that pipes requests")]
struct Args {
    #[arg(short, long, default_value_t = 4000)]
    port: u16,
}

#[derive(Deserialize)]
struct PipeQuery {
    origin: String,
}

async fn pipe_handler(Query(query): Query<PipeQuery>) -> Response {
    let client = reqwest::Client::new();

    match client.get(&query.origin).send().await {
        Ok(response) => {
            let status = StatusCode::from_u16(response.status().as_u16())
                .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
            let headers = response.headers().clone();

            match response.bytes().await {
                Ok(body) => {
                    let mut res = (status, body).into_response();
                    for (key, value) in headers.iter() {
                        res.headers_mut().insert(key, value.clone());
                    }
                    res
                }
                Err(e) => {
                    (StatusCode::BAD_GATEWAY, format!("Failed to read response body: {}", e))
                        .into_response()
                }
            }
        }
        Err(e) => {
            (StatusCode::BAD_GATEWAY, format!("Failed to fetch origin: {}", e)).into_response()
        }
    }
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    let app = Router::new().route("/pipe", get(pipe_handler));

    let addr = SocketAddr::from(([0, 0, 0, 0], args.port));
    println!("Listening on http://{}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
