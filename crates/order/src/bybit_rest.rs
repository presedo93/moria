use anyhow::{Context, Result};
use hmac::{Hmac, Mac};
use metrics::{counter, histogram};
use rust_decimal::Decimal;
use sha2::Sha256;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::{info, warn};

type HmacSha256 = Hmac<Sha256>;

const RECV_WINDOW: u64 = 5000;

#[derive(Debug)]
pub struct OrderResult {
    pub order_id: String,
    pub status: String,
    pub message: String,
}

pub struct BybitRestClient {
    client: reqwest::Client,
    base_url: String,
    api_key: String,
    api_secret: String,
}

impl BybitRestClient {
    pub fn new(base_url: String, api_key: String, api_secret: String) -> Self {
        Self {
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(15))
                .build()
                .expect("failed to build HTTP client"),
            base_url,
            api_key,
            api_secret,
        }
    }

    #[tracing::instrument(skip(self))]
    pub async fn place_order(
        &self,
        symbol: &str,
        side: &str,
        order_type: &str,
        price: Decimal,
        qty: Decimal,
    ) -> Result<OrderResult> {
        let body = if order_type == "Limit" {
            serde_json::json!({
                "category": "linear",
                "symbol": symbol,
                "side": side,
                "orderType": order_type,
                "qty": qty.to_string(),
                "price": price.to_string(),
                "timeInForce": "GTC",
                "positionIdx": 0
            })
        } else {
            serde_json::json!({
                "category": "linear",
                "symbol": symbol,
                "side": side,
                "orderType": "Market",
                "qty": qty.to_string(),
                "timeInForce": "IOC",
                "positionIdx": 0
            })
        };

        let body_str = body.to_string();
        let timestamp = now_ms();
        let signature = self.sign(timestamp, &body_str);
        let started = std::time::Instant::now();

        let url = format!("{}/v5/order/create", self.base_url);
        info!(%url, %symbol, %side, %order_type, %qty, "Placing order");

        let response = self
            .client
            .post(&url)
            .header("X-BAPI-API-KEY", &self.api_key)
            .header("X-BAPI-SIGN", &signature)
            .header("X-BAPI-SIGN-TYPE", "2")
            .header("X-BAPI-TIMESTAMP", timestamp.to_string())
            .header("X-BAPI-RECV-WINDOW", RECV_WINDOW.to_string())
            .header("Content-Type", "application/json")
            .body(body_str)
            .send()
            .await
            .context("Failed to send order request")?
            .json::<serde_json::Value>()
            .await
            .context("Failed to parse order response")?;
        histogram!("order_bybit_http_latency_seconds").record(started.elapsed().as_secs_f64());

        let ret_code = response["retCode"].as_i64().unwrap_or(-1);
        let ret_msg = response["retMsg"].as_str().unwrap_or("unknown");

        if ret_code == 0 {
            counter!("order_bybit_submit_total", "result" => "ok").increment(1);
            let order_id = response["result"]["orderId"]
                .as_str()
                .unwrap_or("")
                .to_string();
            info!(%order_id, "Order placed successfully");
            Ok(OrderResult {
                order_id,
                status: "Submitted".to_string(),
                message: ret_msg.to_string(),
            })
        } else {
            counter!("order_bybit_submit_total", "result" => "rejected").increment(1);
            warn!(ret_code, %ret_msg, "Order rejected by Bybit");
            Ok(OrderResult {
                order_id: String::new(),
                status: "Rejected".to_string(),
                message: format!("retCode={ret_code}: {ret_msg}"),
            })
        }
    }

    fn sign(&self, timestamp: u64, body: &str) -> String {
        let payload = format!("{}{}{}{}", timestamp, self.api_key, RECV_WINDOW, body);
        let mut mac =
            HmacSha256::new_from_slice(self.api_secret.as_bytes()).expect("HMAC accepts any key");
        mac.update(payload.as_bytes());
        hex::encode(mac.finalize().into_bytes())
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before epoch")
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signing_is_deterministic() {
        let client = BybitRestClient::new(
            "https://api-testnet.bybit.com".into(),
            "test_key".into(),
            "test_secret".into(),
        );

        let sig1 = client.sign(1658385579423, r#"{"category":"linear","symbol":"BTCUSDT"}"#);
        let sig2 = client.sign(1658385579423, r#"{"category":"linear","symbol":"BTCUSDT"}"#);
        assert_eq!(sig1, sig2);
        // Signature should be 64 hex characters (32 bytes)
        assert_eq!(sig1.len(), 64);
    }

    #[test]
    fn different_timestamps_produce_different_signatures() {
        let client = BybitRestClient::new(
            "https://api-testnet.bybit.com".into(),
            "test_key".into(),
            "test_secret".into(),
        );

        let sig1 = client.sign(1000, r#"{"test":"body"}"#);
        let sig2 = client.sign(2000, r#"{"test":"body"}"#);
        assert_ne!(sig1, sig2);
    }

    #[test]
    fn different_bodies_produce_different_signatures() {
        let client = BybitRestClient::new(
            "https://api-testnet.bybit.com".into(),
            "test_key".into(),
            "test_secret".into(),
        );

        let sig1 = client.sign(1000, r#"{"qty":"0.001"}"#);
        let sig2 = client.sign(1000, r#"{"qty":"0.002"}"#);
        assert_ne!(sig1, sig2);
    }
}
