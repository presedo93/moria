# ── Build stage ──────────────────────────────────────────────
FROM rust:1.93.1-trixie AS builder

RUN apt-get update && apt-get install -y protobuf-compiler && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY proto/ proto/
COPY crates/ crates/
COPY migrations/ migrations/

RUN cargo build --release

# ── Runtime stage ────────────────────────────────────────────
FROM debian:trixie-slim AS runtime

RUN apt-get update && apt-get install -y ca-certificates libssl3 && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/moria-market-data /usr/local/bin/
COPY --from=builder /app/target/release/moria-strategy    /usr/local/bin/
COPY --from=builder /app/target/release/moria-order       /usr/local/bin/
COPY --from=builder /app/target/release/moria-risk        /usr/local/bin/
COPY --from=builder /app/target/release/moria-reconciler  /usr/local/bin/
