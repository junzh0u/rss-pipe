# AGENTS.md

Guidance for coding agents working in this repository.

## Build & Development Commands

```bash
cargo build              # Build the project
cargo run                # Run with default port 4000
cargo run -- -p 8080     # Run with custom port

make all                 # Run fmt check, clippy, and tests
make fmt                 # Check formatting (cargo fmt --check)
make clippy              # Lint with pedantic warnings (-W clippy::pedantic -D warnings)
make test                # Run tests
make install             # Install binary locally
```

## Architecture

An axum server that proxies RSS feeds, downloads their `.torrent` enclosures, and rewrites the feed to serve those files locally. See README.md for the user-facing API and options.

The entire application is in `src/main.rs`:

- `Args` (clap): port, download path, base URL, retention days, cache TTL
- Routes: `/` (`index_handler`, web form), `/pipe` (`pipe_handler`), `/cache` (`cache_handler`, lists cached origins), and a `ServeDir` for the download directory, mounted at its path relative to the working directory
- `pipe_handler` fetches the optional exclude list, then the origin feed through a moka cache (`origin_cache`), and passes it to `process_rss`
- `process_rss` streams the XML with quick-xml; `process_enclosure` skips excluded titles, downloads to `<download-path>/<guid>.torrent` unless already present, and rewrites the enclosure URL. Items whose download fails are dropped
- `cleanup_old_files` deletes downloads past the retention period at startup and daily from a background task
- `shutdown_signal` handles SIGTERM and Ctrl-C for graceful shutdown; the binary runs as PID 1 in Docker, where SIGTERM has no default handler

Tests live in the `tests` module at the bottom of `main.rs` and spin up throwaway TCP servers to stand in for origin hosts.

## Guidelines

- Update README.md on non-trivial changes (new features, API changes, new options)
