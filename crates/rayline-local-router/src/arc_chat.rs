//! Receipt-bound prepared Chat transport. The pinned service owns translation.
use crate::{
    AppState, AuthStyle, BoxBody, EndpointConfig, apply_endpoint_headers,
    arc_session::SessionConfig,
};
use anyhow::{Result, anyhow, ensure};
use bytes::Bytes;
use futures::stream;
use http_body_util::{BodyExt, Full, StreamBody};
use hyper::{Response, body::Frame};
use serde::Deserialize;
use serde_json::{Value, json};
use std::collections::VecDeque;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Contract {
    schema_version: String,
    source: String,
    target: String,
    implementation_sha256: String,
}

#[derive(Clone)]
pub(crate) struct Codec {
    config: SessionConfig,
    owner: String,
    token: String,
    pin: String,
}
impl Codec {
    pub(crate) fn from_receipt(
        config: &SessionConfig,
        owner: &str,
        token: &str,
        receipt: &Value,
    ) -> Result<Self> {
        let pin = config
            .codec_sha256
            .as_ref()
            .ok_or_else(|| anyhow!("ARC Chat codec not enabled"))?;
        let contract: Contract = serde_json::from_value(receipt["response_codec"].clone())?;
        ensure!(
            receipt["request_format"] == "openai_chat"
                && contract.schema_version == "rayline.arc.response-codec.v1"
                && contract.source == "openai_chat"
                && contract.target == "anthropic_messages"
                && &contract.implementation_sha256 == pin,
            "ARC response codec identity mismatch"
        );
        Ok(Self {
            config: config.clone(),
            owner: owner.into(),
            token: token.into(),
            pin: pin.clone(),
        })
    }
    async fn call(&self, operation: &str, extra: Value) -> Result<Value> {
        let mut payload = json!({"owner_id":self.owner,"session_token":self.token,"implementation_sha256":self.pin,"operation":operation});
        payload
            .as_object_mut()
            .ok_or_else(|| anyhow!("codec payload invalid"))?
            .extend(
                extra
                    .as_object()
                    .ok_or_else(|| anyhow!("codec arguments invalid"))?
                    .clone(),
            );
        let result = self.config.call("codec", &payload).await?;
        ensure!(
            result["implementation_sha256"] == self.pin,
            "ARC codec reply pin mismatch"
        );
        Ok(result)
    }
}

pub(crate) async fn forward(
    state: &AppState,
    endpoint: &EndpointConfig,
    body: Value,
    codec: Codec,
) -> Result<Response<BoxBody>> {
    let streaming = body.get("stream").and_then(Value::as_bool).unwrap_or(false);
    let request = state
        .http
        .post(format!(
            "{}/chat/completions",
            endpoint.base_url.trim_end_matches('/')
        ))
        .json(&body);
    let mut upstream = apply_endpoint_headers(request, endpoint, AuthStyle::Bearer)?
        .send()
        .await?;
    ensure!(
        upstream.status().is_success(),
        "ARC Chat provider returned HTTP {}",
        upstream.status()
    );
    if !streaming {
        let mut bytes = Vec::new();
        while let Some(chunk) = upstream.chunk().await? {
            ensure!(
                bytes.len().saturating_add(chunk.len()) <= codec.config.max_response_bytes,
                "ARC Chat response limit exceeded"
            );
            bytes.extend_from_slice(&chunk);
        }
        let provider: Value = serde_json::from_slice(&bytes)?;
        ensure!(
            provider.get("error").is_none()
                && provider["choices"].as_array().is_some_and(|v| v.len() == 1)
                && provider["choices"][0]["finish_reason"].as_str().is_some(),
            "ARC Chat response incomplete"
        );
        let native = codec.call("response", json!({"body":provider})).await?;
        ensure!(native["body"].is_object(), "ARC codec body missing");
        let bytes = serde_json::to_vec(&native["body"])?;
        ensure!(
            bytes.len() <= codec.config.max_response_bytes,
            "ARC native response limit exceeded"
        );
        return Ok(Response::builder()
            .header("content-type", "application/json")
            .body(
                Full::new(Bytes::from(bytes))
                    .map_err(|never| match never {})
                    .boxed(),
            )?);
    }
    let initial = codec.call("stream_start", json!({})).await?;
    let mut state = ChatStream {
        upstream,
        codec,
        buffer: Vec::new(),
        ready: VecDeque::new(),
        held: Vec::new(),
        sequence: 0,
        finished: false,
        done: false,
        received: 0,
        native_received: 0,
    };
    if initial.get("frames").is_some() {
        state.native(initial, false)?;
    }
    let frames = stream::try_unfold(state, |mut state| async move {
        loop {
            if let Some(frame) = state.ready.pop_front() {
                return Ok::<_, std::io::Error>(Some((Frame::data(Bytes::from(frame)), state)));
            }
            if state.done {
                return Ok(None);
            }
            state
                .advance()
                .await
                .map_err(|_| std::io::Error::other("ARC Chat stream or pinned codec failed"))?;
        }
    });
    Ok(Response::builder()
        .header("content-type", "text/event-stream")
        .body(StreamBody::new(frames).boxed())?)
}

struct ChatStream {
    upstream: reqwest::Response,
    codec: Codec,
    buffer: Vec<u8>,
    ready: VecDeque<String>,
    held: Vec<String>,
    sequence: u64,
    finished: bool,
    done: bool,
    received: usize,
    native_received: usize,
}
fn data(frame: &str) -> String {
    frame
        .lines()
        .filter_map(|line| line.strip_prefix("data:").map(str::trim_start))
        .collect::<Vec<_>>()
        .join("\n")
}
impl ChatStream {
    fn native(&mut self, reply: Value, release: bool) -> Result<()> {
        let frames = reply["frames"]
            .as_array()
            .ok_or_else(|| anyhow!("codec frames missing"))?;
        for frame in frames {
            let frame = frame
                .as_str()
                .ok_or_else(|| anyhow!("codec frame invalid"))?;
            ensure!(
                frame.ends_with("\n\n") || frame.ends_with("\r\n\r\n"),
                "codec frame delimiter missing"
            );
            self.native_received = self.native_received.saturating_add(frame.len());
            ensure!(
                self.native_received <= self.codec.config.max_response_bytes,
                "ARC native stream limit exceeded"
            );
            let value: Value = serde_json::from_str(&data(frame))?;
            ensure!(value["type"] != "error", "codec error event");
            if value["type"] == "message_stop" || !self.held.is_empty() {
                self.held.push(frame.into());
            } else {
                self.ready.push_back(frame.into());
            }
        }
        if release {
            ensure!(
                self.held
                    .iter()
                    .any(|f| serde_json::from_str::<Value>(&data(f))
                        .is_ok_and(|v| v["type"] == "message_stop")),
                "codec terminal missing"
            );
            self.ready.extend(self.held.drain(..));
        }
        Ok(())
    }
    async fn advance(&mut self) -> Result<()> {
        loop {
            let lf = self
                .buffer
                .windows(2)
                .position(|w| w == b"\n\n")
                .map(|n| (n, 2));
            let crlf = self
                .buffer
                .windows(4)
                .position(|w| w == b"\r\n\r\n")
                .map(|n| (n, 4));
            let end = lf.into_iter().chain(crlf).min_by_key(|(offset, _)| *offset);
            if let Some((end, delimiter)) = end {
                let bytes = self.buffer.drain(..end + delimiter).collect::<Vec<_>>();
                let frame = String::from_utf8(bytes)?;
                let data = data(&frame);
                if data.is_empty() {
                    continue;
                }
                if data == "[DONE]" {
                    ensure!(
                        self.finished && self.buffer.iter().all(u8::is_ascii_whitespace),
                        "provider terminal before finish or trailing data"
                    );
                } else {
                    ensure!(!self.done, "provider frame after terminal");
                    let value: Value = serde_json::from_str(&data)?;
                    ensure!(value.get("error").is_none(), "provider stream error");
                    if let Some(choices) = value["choices"].as_array() {
                        ensure!(choices.len() <= 1, "multiple provider choices unsupported");
                        if let Some(choice) = choices.first() {
                            self.finished |= choice["finish_reason"].as_str().is_some();
                        }
                    }
                }
                let reply = self
                    .codec
                    .call(
                        "stream_push",
                        json!({"sequence":self.sequence,"frame":frame}),
                    )
                    .await?;
                self.sequence += 1;
                self.native(reply, false)?;
                if data == "[DONE]" {
                    let reply = self
                        .codec
                        .call("stream_finish", json!({"sequence":self.sequence}))
                        .await?;
                    self.native(reply, true)?;
                    self.done = true;
                }
                return Ok(());
            }
            let chunk = self
                .upstream
                .chunk()
                .await?
                .ok_or_else(|| anyhow!("provider stream missing complete terminal"))?;
            self.received = self.received.saturating_add(chunk.len());
            ensure!(
                self.received <= self.codec.config.max_response_bytes,
                "provider stream exceeds bound"
            );
            self.buffer.extend_from_slice(&chunk);
        }
    }
}

#[cfg(test)]
#[path = "arc_chat_tests.rs"]
mod tests;
