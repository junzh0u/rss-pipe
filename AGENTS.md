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

This is a simple HTTP proxy server built with axum. It has a single endpoint:

- `GET /pipe?origin={URL}` - Fetches the given URL and returns the response unchanged (status, headers, body)

The entire application is in `src/main.rs`:
- CLI argument parsing via clap (port configuration)
- axum router with one route
- `pipe_handler` uses reqwest to fetch the origin URL and passes through the response

## Guidelines

- Update README.md on non-trivial changes (new features, API changes, new options)
