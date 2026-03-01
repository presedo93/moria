use anyhow::{Context, Result};
use futures::{SinkExt, StreamExt};
use moria_proto::market_data::{Kline, Level, OrderbookSnapshot, Trade};
use serde::Deserialize;
use std::time::Duration;
use tokio::sync::broadcast;
use tokio::time::{self, Instant};
use tokio_tungstenite::{connect_async, tungstenite::Message as WsMessage};
use tracing::{error, info, warn};

const PING_INTERVAL: Duration = Duration::from_secs(20);
const RECONNECT_BASE_DELAY: Duration = Duration::from_secs(1);
const MAX_RECONNECT_DELAY: Duration = Duration::from_secs(60);

pub struct BybitWs {
    url: String,
    symbol: String,
    interval: String,
    kline_tx: broadcast::Sender<Kline>,
    trade_tx: broadcast::Sender<Trade>,
    orderbook_tx: broadcast::Sender<OrderbookSnapshot>,
}

impl BybitWs {
    pub fn new(
        url: String,
        symbol: String,
        interval: String,
        kline_tx: broadcast::Sender<Kline>,
        trade_tx: broadcast::Sender<Trade>,
        orderbook_tx: broadcast::Sender<OrderbookSnapshot>,
    ) -> Self {
        Self {
            url,
            symbol,
            interval,
            kline_tx,
            trade_tx,
            orderbook_tx,
        }
    }

    pub async fn run(&self) -> ! {
        let mut attempt = 0u32;

        loop {
            match self.connect_and_stream().await {
                Ok(()) => {
                    info!("WebSocket connection closed normally, reconnecting");
                    attempt = 0;
                }
                Err(e) => {
                    attempt = attempt.saturating_add(1);
                    let delay = std::cmp::min(
                        RECONNECT_BASE_DELAY * 2u32.saturating_pow(attempt - 1),
                        MAX_RECONNECT_DELAY,
                    );
                    error!(?e, attempt, ?delay, "WebSocket error, reconnecting");
                    time::sleep(delay).await;
                }
            }
        }
    }

    async fn connect_and_stream(&self) -> Result<()> {
        info!(url = %self.url, "Connecting to Bybit WebSocket");
        let (ws_stream, _) = connect_async(&self.url)
            .await
            .context("Failed to connect to Bybit WS")?;

        let (mut write, mut read) = ws_stream.split();
        info!("Connected to Bybit WebSocket");

        // Subscribe to topics
        let sub_msg = serde_json::json!({
            "op": "subscribe",
            "args": [
                format!("kline.{}.{}", self.interval, self.symbol),
                format!("publicTrade.{}", self.symbol),
                format!("orderbook.50.{}", self.symbol),
            ]
        });
        write
            .send(WsMessage::Text(sub_msg.to_string().into()))
            .await
            .context("Failed to send subscribe")?;
        info!(symbol = %self.symbol, interval = %self.interval, "Subscribed to topics");

        let mut last_ping = Instant::now();

        loop {
            // Send ping if needed
            if last_ping.elapsed() >= PING_INTERVAL {
                let ping = serde_json::json!({"op": "ping"});
                write
                    .send(WsMessage::Text(ping.to_string().into()))
                    .await
                    .context("Failed to send ping")?;
                last_ping = Instant::now();
            }

            // Read with timeout so we can send pings
            let msg = tokio::select! {
                msg = read.next() => msg,
                _ = time::sleep(PING_INTERVAL) => continue,
            };

            let msg = match msg {
                Some(Ok(msg)) => msg,
                Some(Err(e)) => return Err(e.into()),
                None => return Ok(()), // stream ended
            };

            match msg {
                WsMessage::Text(text) => self.handle_message(&text),
                WsMessage::Ping(data) => {
                    write.send(WsMessage::Pong(data)).await?;
                }
                WsMessage::Close(_) => return Ok(()),
                _ => {}
            }
        }
    }

    fn handle_message(&self, text: &str) {
        let envelope: WsEnvelope = match serde_json::from_str(text) {
            Ok(e) => e,
            Err(_) => {
                // Likely a subscription response or pong — not an error
                tracing::trace!(msg = text, "Non-data message received");
                return;
            }
        };

        let Some(topic) = &envelope.topic else {
            return;
        };

        if topic.starts_with("kline.") {
            self.handle_kline(text);
        } else if topic.starts_with("publicTrade.") {
            self.handle_trade(text);
        } else if topic.starts_with("orderbook.") {
            self.handle_orderbook(text);
        }
    }

    fn handle_kline(&self, text: &str) {
        let msg: Result<KlineMsg, _> = serde_json::from_str(text);
        match msg {
            Ok(msg) => {
                for k in msg.data {
                    let kline = Kline {
                        symbol: self.symbol.clone(),
                        interval: k.interval,
                        open: k.open.parse().unwrap_or(0.0),
                        high: k.high.parse().unwrap_or(0.0),
                        low: k.low.parse().unwrap_or(0.0),
                        close: k.close.parse().unwrap_or(0.0),
                        volume: k.volume.parse().unwrap_or(0.0),
                        timestamp: k.start,
                    };
                    let _ = self.kline_tx.send(kline);
                }
            }
            Err(e) => warn!(?e, "Failed to parse kline message"),
        }
    }

    fn handle_trade(&self, text: &str) {
        let msg: Result<TradeMsg, _> = serde_json::from_str(text);
        match msg {
            Ok(msg) => {
                for t in msg.data {
                    let trade = Trade {
                        symbol: t.s,
                        price: t.p.parse().unwrap_or(0.0),
                        qty: t.v.parse().unwrap_or(0.0),
                        side: t.side_upper,
                        timestamp: t.timestamp,
                    };
                    let _ = self.trade_tx.send(trade);
                }
            }
            Err(e) => warn!(?e, "Failed to parse trade message"),
        }
    }

    fn handle_orderbook(&self, text: &str) {
        let msg: Result<OrderbookMsg, _> = serde_json::from_str(text);
        match msg {
            Ok(msg) => {
                let snapshot = OrderbookSnapshot {
                    symbol: msg.data.s,
                    bids: msg
                        .data
                        .b
                        .into_iter()
                        .map(|l| Level {
                            price: l[0].parse().unwrap_or(0.0),
                            qty: l[1].parse().unwrap_or(0.0),
                        })
                        .collect(),
                    asks: msg
                        .data
                        .a
                        .into_iter()
                        .map(|l| Level {
                            price: l[0].parse().unwrap_or(0.0),
                            qty: l[1].parse().unwrap_or(0.0),
                        })
                        .collect(),
                    timestamp: msg.ts,
                };
                let _ = self.orderbook_tx.send(snapshot);
            }
            Err(e) => warn!(?e, "Failed to parse orderbook message"),
        }
    }
}

// --- Bybit WS JSON types ---

#[derive(Deserialize)]
struct WsEnvelope {
    topic: Option<String>,
}

#[derive(Deserialize)]
struct KlineMsg {
    data: Vec<KlineData>,
}

#[derive(Deserialize)]
struct KlineData {
    start: i64,
    interval: String,
    open: String,
    high: String,
    low: String,
    close: String,
    volume: String,
}

#[derive(Deserialize)]
struct TradeMsg {
    data: Vec<TradeData>,
}

#[derive(Deserialize)]
struct TradeData {
    #[serde(rename = "T")]
    timestamp: i64,
    s: String,
    #[serde(rename = "S")]
    side_upper: String,
    v: String,
    p: String,
}

#[derive(Deserialize)]
struct OrderbookMsg {
    ts: i64,
    data: OrderbookData,
}

#[derive(Deserialize)]
struct OrderbookData {
    s: String,
    b: Vec<Vec<String>>,
    a: Vec<Vec<String>>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_kline_message() {
        let json = r#"{
            "topic": "kline.5.BTCUSDT",
            "type": "snapshot",
            "ts": 1672324988882,
            "data": [{
                "start": 1672324800000,
                "end": 1672325099999,
                "interval": "5",
                "open": "16649.5",
                "close": "16677",
                "high": "16677",
                "low": "16608",
                "volume": "2.081",
                "turnover": "34666.4005",
                "confirm": false,
                "timestamp": 1672324988882
            }]
        }"#;

        let msg: KlineMsg = serde_json::from_str(json).unwrap();
        assert_eq!(msg.data.len(), 1);
        assert_eq!(msg.data[0].open, "16649.5");
        assert_eq!(msg.data[0].close, "16677");
        assert_eq!(msg.data[0].interval, "5");
    }

    #[test]
    fn parse_trade_message() {
        let json = r#"{
            "topic": "publicTrade.BTCUSDT",
            "type": "snapshot",
            "ts": 1672304486868,
            "data": [{
                "T": 1672304486865,
                "s": "BTCUSDT",
                "S": "Buy",
                "v": "0.001",
                "p": "16578.50",
                "L": "PlusTick",
                "i": "20f43950-d8dd-5b31-9112-a178eb6023af",
                "BT": false
            }]
        }"#;

        let msg: TradeMsg = serde_json::from_str(json).unwrap();
        assert_eq!(msg.data.len(), 1);
        assert_eq!(msg.data[0].s, "BTCUSDT");
        assert_eq!(msg.data[0].p, "16578.50");
        assert_eq!(msg.data[0].side_upper, "Buy");
    }

    #[test]
    fn parse_orderbook_message() {
        let json = r#"{
            "topic": "orderbook.50.BTCUSDT",
            "type": "snapshot",
            "ts": 1672304484978,
            "data": {
                "s": "BTCUSDT",
                "b": [["16493.50", "0.006"], ["16493.00", "0.100"]],
                "a": [["16611.00", "0.029"], ["16612.00", "0.213"]],
                "u": 18521288,
                "seq": 7961638724
            }
        }"#;

        let msg: OrderbookMsg = serde_json::from_str(json).unwrap();
        assert_eq!(msg.data.s, "BTCUSDT");
        assert_eq!(msg.data.b.len(), 2);
        assert_eq!(msg.data.a.len(), 2);
        assert_eq!(msg.data.b[0][0], "16493.50");
    }

    #[test]
    fn parse_envelope_with_topic() {
        let json = r#"{"topic": "kline.1.BTCUSDT", "type": "snapshot"}"#;
        let env: WsEnvelope = serde_json::from_str(json).unwrap();
        assert_eq!(env.topic.as_deref(), Some("kline.1.BTCUSDT"));
    }

    #[test]
    fn parse_envelope_without_topic() {
        let json = r#"{"success": true, "ret_msg": "pong", "op": "ping"}"#;
        let env: WsEnvelope = serde_json::from_str(json).unwrap();
        assert!(env.topic.is_none());
    }
}
