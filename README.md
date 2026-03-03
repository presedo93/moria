# Moria Trading Engine

A Rust gRPC microservices trading engine that reads real-time market data from Bybit (testnet), processes it through configurable strategies, validates orders through a risk management layer, and places orders via the Bybit REST API. All services export distributed traces via OpenTelemetry.

## Architecture

```
Bybit WebSocket v5                                          Bybit REST v5
       │                                                         ▲
       │ klines, trades, orderbook                               │ POST /v5/order/create
       ▼                                                         │
┌──────────────────┐          ┌──────────────┐          ┌────────────────┐
│  Market Data     │ StreamKlines │  Strategy    │ ValidateOrder │    Risk        │ PlaceOrder  │   Order       │
│  :50051          │◄─────────│  (client)    │──────────►│    :50053       │────────────►│   :50054      │
│                  │  gRPC     │              │   gRPC    │                │    gRPC      │               │
│  Bybit WS ─►    │  stream   │  SMA cross.  │          │  pos limits    │             │  HMAC-SHA256  │
│  broadcast ch.   │          │  signal gen. │          │  daily loss    │             │  signing      │
└──────────────────┘          └──────────────┘          └───────┬────────┘             └───────────────┘
                                                                │
                                                                ▼
                                                         ┌──────────────┐
                                                         │  PostgreSQL  │
                                                         │  signals     │
                                                         │  trades      │
                                                         │  positions   │
                                                         └──────────────┘
```

### Services

| Service | Port | Role |
|---------|------|------|
| **market-data** | 50051 | Connects to Bybit WebSocket v5, broadcasts klines/trades/orderbook via gRPC server streaming |
| **strategy** | client-only | Subscribes to kline stream, runs SMA crossover detection, emits order signals |
| **risk** | 50053 | Validates orders against position limits and daily loss, persists decisions to PostgreSQL |
| **order** | 50054 | Signs and sends orders to Bybit REST v5 API |

### Data Flow

1. **market-data** connects to Bybit WebSocket v5 testnet, subscribes to `kline.{interval}.{symbol}`, `publicTrade.{symbol}`, `orderbook.50.{symbol}`
2. **strategy** subscribes to the kline stream via gRPC, maintains a rolling price window, computes short/long SMA
3. When the short SMA crosses above the long SMA, a **Buy** signal is emitted; crossing below emits a **Sell** signal
4. **risk** receives the signal, checks position size and daily loss limits, persists the decision
5. Approved orders are forwarded to **order**, which signs the request with HMAC-SHA256 and POSTs to Bybit

## Quick Start

### Prerequisites

- [Rust 1.85+](https://rustup.rs/)
- [Docker & Docker Compose](https://docs.docker.com/get-docker/)
- [protoc](https://grpc.io/docs/protoc-installation/) (Protocol Buffers compiler)
- A [Bybit testnet](https://testnet.bybit.com/) API key

### 1. Clone and configure

```bash
git clone <repo-url> moria && cd moria
cp .env.example .env
```

Edit `.env` with your Bybit testnet credentials:

```
BYBIT_API_KEY=your_testnet_api_key
BYBIT_API_SECRET=your_testnet_api_secret
```

### 2. Run with Docker Compose

```bash
docker-compose up --build
```

This starts all 4 application services plus:

- **PostgreSQL** (port 5432) — trade/signal persistence
- **OTel Collector** (port 4317) — receives traces from all services
- **Jaeger** (port 16686) — trace visualization UI

### 3. Verify

```bash
# Check market data streaming
grpcurl -plaintext localhost:50051 market_data.MarketDataService/StreamKlines

# Check database records
psql postgres://moria:moria@localhost:5432/moria -c \
  "SELECT * FROM signals ORDER BY created_at DESC LIMIT 5;"

# View distributed traces
open http://localhost:16686
```

### 4. Verify Health & Metrics

```bash
# gRPC health checks
grpcurl -plaintext -d '{"service":"market_data.MarketDataService"}' \
  localhost:50051 grpc.health.v1.Health/Check
grpcurl -plaintext -d '{"service":"risk.RiskService"}' \
  localhost:50053 grpc.health.v1.Health/Check
grpcurl -plaintext -d '{"service":"order.OrderService"}' \
  localhost:50054 grpc.health.v1.Health/Check

# Prometheus metrics endpoints (docker-compose defaults)
curl -fsS http://localhost:9101/metrics | head
curl -fsS http://localhost:9102/metrics | head
curl -fsS http://localhost:9103/metrics | head
curl -fsS http://localhost:9104/metrics | head
```

### Running locally (without Docker)

```bash
# Start infrastructure only
docker-compose up postgres otel-collector jaeger

# Build all services
cargo build --release

# Run each service (in separate terminals)
source .env
./target/release/moria-market-data
./target/release/moria-order
./target/release/moria-risk
./target/release/moria-strategy
```

## Project Structure

```
moria/
├── proto/                        # Protobuf service definitions
│   ├── market_data.proto         #   MarketDataService (StreamKlines, StreamTrades, StreamOrderbook)
│   ├── strategy.proto            #   Signal/SignalAck messages
│   ├── order.proto               #   OrderService (PlaceOrder)
│   └── risk.proto                #   RiskService (ValidateOrder)
├── crates/
│   ├── proto/                    # Generated gRPC code (tonic-build)
│   ├── common/                   # Shared config, telemetry initialization
│   ├── market-data/              # Bybit WS → gRPC streaming
│   │   ├── src/bybit_ws.rs       #   WebSocket connection, reconnect, message parsing
│   │   └── src/server.rs         #   gRPC server with broadcast channels
│   ├── strategy/                 # Signal generation
│   │   ├── src/sma.rs            #   SMA computation and crossover detection
│   │   └── src/engine.rs         #   Kline consumer, signal emitter
│   ├── risk/                     # Risk management + persistence
│   │   ├── src/validator.rs      #   Position and loss limit checks
│   │   ├── src/db.rs             #   PostgreSQL queries (signals, trades, positions)
│   │   └── src/server.rs         #   gRPC server, approval/rejection flow
│   └── order/                    # Order execution
│       ├── src/bybit_rest.rs     #   Bybit v5 REST client with HMAC-SHA256 signing
│       └── src/server.rs         #   gRPC server
├── migrations/
│   └── 001_initial.sql           # signals, trades, positions tables
├── docker-compose.yml            # Full stack deployment
├── Dockerfile                    # Multi-stage build (rust:1.85 → debian-slim)
└── otel-collector-config.yaml    # OTLP → Jaeger trace pipeline
```

## Configuration

All services are configured via environment variables. See [`.env.example`](.env.example) for the full list.

| Variable | Default | Description |
|----------|---------|-------------|
| `BYBIT_API_KEY` | — | Bybit API key (required for order placement) |
| `BYBIT_API_SECRET` | — | Bybit API secret (required for order placement) |
| `BYBIT_WS_URL` | `wss://stream-testnet.bybit.com/v5/public/linear` | WebSocket endpoint |
| `BYBIT_REST_URL` | `https://api-testnet.bybit.com` | REST API endpoint |
| `TRADING_PAIR` | `BTCUSDT` | Trading pair symbol |
| `KLINE_INTERVAL` | `1` | Kline interval in minutes |
| `SMA_SHORT_PERIOD` | `10` | Short SMA window (candles) |
| `SMA_LONG_PERIOD` | `30` | Long SMA window (candles) |
| `ORDER_QTY` | `0.001` | Order quantity submitted per generated signal |
| `MAX_POSITION_SIZE` | `1.0` | Maximum position size (base asset) |
| `MAX_DAILY_LOSS` | `100.0` | Maximum daily loss (quote asset) |
| `DATABASE_URL` | `postgres://moria:moria@localhost:5432/moria` | PostgreSQL connection |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://localhost:4317` | OTel collector (omit to disable export) |
| `METRICS_ADDR` | — | Optional Prometheus metrics listen address (example: `0.0.0.0:9103`) |
| `MARKET_DATA_GRPC_ADDR` | `[::1]:50051` | Market data service listen address |
| `RISK_GRPC_ADDR` | `[::1]:50053` | Risk service listen address |
| `ORDER_GRPC_ADDR` | `[::1]:50054` | Order service listen address |

## Strategy: SMA Crossover

The default strategy computes two Simple Moving Averages over close prices:

- **Short SMA** (default 10 periods) — fast-moving, responsive to recent prices
- **Long SMA** (default 30 periods) — slow-moving, smooths out noise

Signals are generated when the SMAs cross:

| Condition | Signal |
|-----------|--------|
| Short SMA crosses **above** Long SMA | **Buy** (bullish momentum) |
| Short SMA crosses **below** Long SMA | **Sell** (bearish momentum) |
| No crossover | No signal |

The strategy requires at least `SMA_LONG_PERIOD` candles before generating any signals.

## Risk Management

Every order signal passes through the risk service before execution:

| Rule | Behavior |
|------|----------|
| **Position limit** | Rejects if resulting position after side-aware delta (`Buy` adds qty, `Sell` subtracts qty) exceeds `±MAX_POSITION_SIZE` |
| **Daily loss limit** | Rejects if realized daily PnL is below `-MAX_DAILY_LOSS` |

All decisions (approved and rejected) are persisted to the `signals` table with a reason field for rejected orders.

Duplicate `signal_id` requests are treated idempotently: risk returns the previously persisted decision and does not place a second order.

## Database Schema

```sql
signals    — Every order signal with approval status and rejection reason
trades     — Executed trades linked to their originating signal
positions  — Current position per symbol (qty, avg entry price)
```

Migrations are applied automatically on risk service startup.

## Observability

When `OTEL_EXPORTER_OTLP_ENDPOINT` is set, all services export traces via OTLP/gRPC:

- Every gRPC call creates a span with service name, method, and structured fields (symbol, side, price)
- Traces flow end-to-end: market data event → strategy signal → risk decision → order placement
- **Jaeger UI** at `http://localhost:16686` visualizes the full trace chain

Log level is controlled via `RUST_LOG` (default: `info,{service}=debug`).

When `METRICS_ADDR` is set, each service exposes Prometheus metrics at `/metrics`.

## Health Checks

The gRPC services (`market-data`, `risk`, `order`) expose `grpc.health.v1.Health/Check` via `tonic-health`.

## Development

```bash
# Build
cargo build

# Test (21 tests)
cargo test

# Lint
cargo clippy

# Build release binaries
cargo build --release
```

### Test Breakdown

| Crate | Tests | Coverage |
|-------|-------|----------|
| market-data | 5 | Bybit WS message parsing (kline, trade, orderbook, envelope) |
| strategy | 7 | SMA computation, crossover detection, edge cases |
| risk | 6 | Position limits, daily loss, boundary conditions |
| order | 3 | HMAC-SHA256 signing determinism and uniqueness |

## Technology Stack

| Component | Crate | Purpose |
|-----------|-------|---------|
| Async runtime | `tokio` | Event loop, task spawning |
| gRPC | `tonic` + `prost` | Service communication |
| WebSocket | `tokio-tungstenite` | Bybit market data feed |
| HTTP client | `reqwest` | Bybit REST order placement |
| Database | `sqlx` (PostgreSQL) | Trade/signal persistence |
| Tracing | `tracing` + `tracing-opentelemetry` | Structured logging + OTel |
| OTel export | `opentelemetry-otlp` | OTLP/gRPC span export |
| Crypto | `hmac` + `sha2` | Bybit API request signing |

## License

This project is for educational and testnet use. Use at your own risk.
