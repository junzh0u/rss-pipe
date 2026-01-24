# rss-pipe

A proxy server that downloads torrent enclosures from RSS feeds and rewrites the feed to serve them locally.

## Features

- Fetches RSS feeds and downloads `.torrent` files from enclosure URLs
- Rewrites enclosure URLs to point to locally served files
- Skips already downloaded torrents (uses GUID as filename)
- Removes items from feed when download fails
- Supports exclude patterns to filter out unwanted items (skips download entirely)
- Serves downloaded files via built-in static file server
- Automatically cleans up old files on startup

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

### GET /pipe

Proxies an RSS feed, downloading enclosures and rewriting URLs.

**Query Parameters:**

| Parameter | Required | Description |
|-----------|----------|-------------|
| `origin` | Yes | URL of the RSS feed to fetch |
| `exclude` | No | URL to a text file containing exclude patterns (one per line) |

**Example:**

```bash
curl "http://localhost:4000/pipe?origin=https://example.com/feed.rss"

# With exclude patterns
curl "http://localhost:4000/pipe?origin=https://example.com/feed.rss&exclude=https://example.com/excludes.txt"
```

Items whose title contains any exclude pattern will be filtered out without attempting download.

### GET /<download-path>/*

Serves downloaded torrent files as static content.

## How It Works

1. Fetches the RSS feed from the `origin` URL
2. Parses each `<item>` element looking for `<enclosure>` tags
3. Skips items whose title matches an exclude pattern (no download attempted)
4. Downloads the enclosure URL to `<download-path>/<guid>.torrent`
5. Rewrites the enclosure URL to `<base-url>/<guid>.torrent`
6. Removes items where download failed
7. Returns the modified RSS feed

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
