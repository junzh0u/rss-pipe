# rss-pipe

A proxy server that downloads torrent enclosures from RSS feeds and rewrites the feed to serve them locally.

## Features

- Fetches RSS feeds and downloads `.torrent` files from enclosure URLs
- Rewrites enclosure URLs to point to locally served files
- Skips already downloaded torrents (uses GUID as filename)
- Removes items from feed when download fails
- Supports exclude patterns to filter out unwanted items (skips download entirely)
- Caches origin feeds to reduce upstream requests
- Serves downloaded files via built-in static file server
- Automatically cleans up old files (at startup and daily)
- Web UI for generating pipe URLs

## Installation

```bash
cargo install --path .
```

## Usage

```bash
rss-pipe [OPTIONS]
```

### Options

| Option | Default | Description |
|--------|---------|-------------|
| `-p, --port <PORT>` | `4000` | Port to listen on |
| `-d, --download-path <PATH>` | `./downloaded/` | Directory to store downloaded torrents (must be under current working directory) |
| `-b, --base-url <URL>` | `http://localhost:<PORT>/<PATH>` | Base URL for rewritten enclosure links |
| `-r, --retention-days <DAYS>` | `30` | Delete downloaded files older than this many days on startup |
| `-c, --cache-ttl-minutes <MINS>` | `10` | Cache origin RSS feeds for this many minutes |

### Examples

```bash
# Run with defaults (port 4000, downloads to ./downloaded/)
rss-pipe

# Custom port
rss-pipe -p 8080

# Custom download path
rss-pipe -d ./torrents/

# With external base URL (for reverse proxy setups)
rss-pipe -b https://example.com

# Keep files for 7 days instead of 30
rss-pipe -r 7
```

## API

### GET /

Web UI with a form to generate pipe URLs. Enter the origin RSS URL and optional exclude URL, then click "Pipe" to navigate to the processed feed.

### GET /pipe

Proxies an RSS feed, downloading enclosures and rewriting URLs.

**Query Parameters:**

| Parameter | Required | Description |
|-----------|----------|-------------|
| `origin` | Yes | URL of the RSS feed to fetch |
| `exclude` | No | URL to a text file containing exclude patterns (one per line) |

**Example:**

```bash
curl 'http://localhost:4000/pipe?origin=https%3A%2F%2Fexample.com%2Ffeed.rss'

# With exclude patterns
curl 'http://localhost:4000/pipe?origin=https%3A%2F%2Fexample.com%2Ffeed.rss&exclude=https%3A%2F%2Fexample.com%2Fexcludes.txt'
```

Items whose title contains any exclude pattern will be filtered out without attempting download.

### GET /cache

Displays a list of cached origin feeds with their remaining TTL.

Shows:
- All currently cached origin URLs (clickable links to `/pipe`)
- Time remaining before each entry expires

### GET /downloaded/*

Serves downloaded torrent files as static content. The path matches the `--download-path` option (default: `downloaded`).

## How It Works

1. Fetches the RSS feed from the `origin` URL
2. Parses each `<item>` element looking for `<enclosure>` tags
3. Skips items whose title matches an exclude pattern (no download attempted)
4. Downloads the enclosure URL to `<download-path>/<guid>.torrent`
5. Rewrites the enclosure URL to point to the local file
6. Removes items where download failed
7. Returns the modified RSS feed

## Docker

```bash
# Copy and configure environment
cp env.example .env
# Edit .env with your settings

# Run with docker compose
docker compose up -d
```

Configuration options in `.env`:

| Variable | Default | Description |
|----------|---------|-------------|
| `PORT` | `4000` | Port to listen on |
| `BASE_URL` | (none) | Base URL for external access |
| `RETENTION_DAYS` | `30` | Days to keep downloaded files |
| `CACHE_TTL_MINUTES` | `10` | Minutes to cache origin feeds |

## Development

```bash
# Build
cargo build

# Run tests
cargo test

# Run all checks (fmt, clippy, test)
make all
```

## License

MIT
