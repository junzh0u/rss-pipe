FROM rust:1.97-alpine AS builder

RUN apk add --no-cache musl-dev

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src

RUN cargo build --release

FROM alpine:3.21

RUN adduser -D -u 1000 appuser
USER appuser

WORKDIR /app

COPY --from=builder /app/target/release/rss-pipe /usr/local/bin/rss-pipe

EXPOSE 4000

ENTRYPOINT ["rss-pipe"]
