//! Opt-in native Messages transport for an operator-managed ARC session service.
//! The service owns policy state; this adapter owns provider transport settlement.
use crate::{AppState, BoxBody, RouteDecision, RouteSelection, arc::ArcConfig};
use anyhow::{Result, anyhow, ensure};
use futures::stream;
use http_body_util::{BodyExt, StreamBody};
use hyper::{HeaderMap, Response};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::sync::{
    Arc, OnceLock,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::time::Duration;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionConfig {
    pub base_url: String,
    pub timeout_ms: u64,
    pub max_response_bytes: usize,
    #[serde(skip)]
    runtime: Arc<RuntimeState>,
}

#[derive(Debug, Default)]
struct RuntimeState {
    uncertain: AtomicBool,
    aborting: AtomicUsize,
    gates: std::sync::Mutex<
        std::collections::HashMap<String, std::sync::Weak<tokio::sync::Mutex<()>>>,
    >,
}

impl SessionConfig {
    fn url(&self, operation: &str) -> Result<reqwest::Url> {
        let url = reqwest::Url::parse(&self.base_url)?;
        ensure!(
            url.scheme() == "http"
                && url
                    .host_str()
                    .and_then(|s| s.trim_matches(['[', ']']).parse::<std::net::IpAddr>().ok())
                    .is_some_and(|ip| ip.is_loopback())
                && url.username().is_empty()
                && url.password().is_none()
                && url.query().is_none()
                && url.fragment().is_none()
                && url.path() == "/",
            "ARC session service must be a literal HTTP loopback origin"
        );
        Ok(url.join(&format!("/experimental/arc/session/{operation}"))?)
    }
    pub(crate) fn validate(&self, arc: &ArcConfig) -> Result<()> {
        self.url("prepare")?;
        ensure!(
            self.timeout_ms > 0 && (1..=16 * 1024 * 1024).contains(&self.max_response_bytes),
            "ARC session timeout and bounded response size required"
        );
        // Stage 2 is private text steering. Native controls belong to the worker.
        for a in arc.bindings.values() {
            for b in arc.bindings.values() {
                if a.target.endpoint == b.target.endpoint && a.target.model == b.target.model {
                    ensure!(
                        a.request_overrides == b.request_overrides,
                        "ARC session actions for one worker must have identical native controls"
                    );
                }
            }
        }
        Ok(())
    }
    async fn call(&self, operation: &str, payload: &Value) -> Result<Value> {
        let client = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_millis(self.timeout_ms))
            .build()?;
        let mut response = client
            .post(self.url(operation)?)
            .json(payload)
            .send()
            .await?;
        ensure!(
            response.status().is_success(),
            "ARC session {operation} returned HTTP {}",
            response.status()
        );
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await? {
            ensure!(
                bytes.len().saturating_add(chunk.len()) <= 16 * 1024 * 1024,
                "ARC session receipt exceeds limit"
            );
            bytes.extend_from_slice(&chunk);
        }
        Ok(serde_json::from_slice(&bytes)?)
    }
}

fn native_identity(headers: &HeaderMap, body: &Value) -> Result<Value> {
    let header = |name| {
        headers
            .get(name)
            .and_then(|v| v.to_str().ok())
            .filter(|v| !v.is_empty())
            .map(str::to_owned)
    };
    let user = body
        .pointer("/metadata/user_id")
        .and_then(Value::as_str)
        .and_then(|v| serde_json::from_str::<Value>(v).ok());
    let session = header("x-client-session-id")
        .or_else(|| header("x-claude-code-session-id"))
        .or_else(|| {
            user.as_ref()?
                .get("session_id")?
                .as_str()
                .filter(|s| !s.is_empty())
                .map(str::to_owned)
        });
    let session = session.ok_or_else(|| anyhow!("ARC session mode requires native x-client-session-id or metadata.user_id JSON session_id; anonymous histories cannot be safely joined"))?;
    Ok(
        json!({"session_id":session,"agent_id":header("x-claude-code-agent-id"),"parent_agent_id":header("x-claude-code-parent-agent-id")}),
    )
}

pub(crate) async fn prepare(
    state: &AppState,
    headers: &HeaderMap,
    body: &mut Value,
) -> Result<(RouteDecision, Turn)> {
    let arc = state
        .config
        .arc
        .as_ref()
        .ok_or_else(|| anyhow!("ARC not configured"))?;
    let config = arc
        .session
        .as_ref()
        .ok_or_else(|| anyhow!("ARC session mode not configured"))?
        .clone();
    let metadata = native_identity(headers, body)?;
    let key = serde_json::to_string(&metadata)?;
    let gate = {
        let mut gates = config
            .runtime
            .gates
            .lock()
            .map_err(|_| anyhow!("ARC session gate unavailable"))?;
        gates.retain(|_, gate| gate.strong_count() > 0);
        if let Some(gate) = gates.get(&key).and_then(std::sync::Weak::upgrade) {
            gate
        } else {
            let gate = Arc::new(tokio::sync::Mutex::new(()));
            gates.insert(key, Arc::downgrade(&gate));
            gate
        }
    };
    let gate = gate.lock_owned().await;
    ensure!(
        !config.runtime.uncertain.load(Ordering::SeqCst),
        "ARC session settlement uncertain; restart with a new owner after resolving the service transaction"
    );
    static OWNER: OnceLock<String> = OnceLock::new();
    let owner = OWNER
        .get_or_init(|| {
            format!(
                "rayline_{}_{}",
                std::process::id(),
                rayline_metrics::new_request_id()
            )
        })
        .clone();
    let operation = rayline_metrics::new_request_id();
    let payload = json!({"owner_id":owner,"operation_id":operation,"metadata":metadata,"request_format":"anthropic_messages","request":body,"available_action_ids":arc.bindings.keys().collect::<Vec<_>>()});
    // A cancelled/ambiguous prepare may have reserved remote state. Do not reuse
    // this owner silently. The guard also covers task cancellation while awaiting.
    let mut pending = PrepareGuard {
        config: config.clone(),
        armed: true,
    };
    let receipt = config.call("prepare", &payload).await?;
    let mut turn = Turn {
        config,
        owner,
        token: receipt
            .get("session_token")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| anyhow!("ARC session receipt missing token"))?
            .to_owned(),
        settled: false,
        accepted_output: None,
        saw_terminal_keyword: false,
        gate: Some(gate),
    };
    pending.armed = false;
    let action = receipt["action_id"]
        .as_str()
        .ok_or_else(|| anyhow!("ARC session action missing"))?;
    let binding = arc
        .bindings
        .get(action)
        .ok_or_else(|| anyhow!("ARC session selected unbound action"))?;
    ensure!(
        receipt["owner_id"] == turn.owner
            && receipt["package_sha256"] == arc.package_sha256
            && receipt["request_format"] == "anthropic_messages"
            && receipt["source_request_format"] == "anthropic_messages",
        "ARC session receipt identity mismatch"
    );
    ensure!(
        receipt["decision"]["package"]
            == json!({"alias":arc.package_alias,"package_sha256":arc.package_sha256})
            && receipt["decision"]["decision"]["selected_action_id"] == action,
        "ARC session numerical decision mismatch"
    );
    arc.validate_response(
        &json!({"selection":{"available_action_ids":arc.bindings.keys().collect::<Vec<_>>()}}),
        &receipt["decision"],
    )?;
    ensure!(
        receipt["decision"]["decision"]["selected_arm_id"]
            .as_str()
            .is_some_and(|s| s.len() == 64 && s.bytes().all(|b| b.is_ascii_hexdigit()))
            && receipt["decision"]["encoding"]["session_revision"]
                .as_u64()
                .is_some(),
        "ARC session requires trained arm and numerical revision"
    );
    let prepared = receipt
        .get("request")
        .filter(|v| v.is_object())
        .ok_or_else(|| anyhow!("ARC session provider request missing"))?;
    ensure!(
        prepared["model"] == binding.target.model && prepared["messages"].is_array(),
        "ARC session provider model/history mismatch"
    );
    for key in ["thinking", "output_config"] {
        ensure!(
            prepared.get(key) == binding.request_overrides.get(key),
            "ARC session changed fixed worker {key}"
        );
    }
    for (key, value) in body
        .as_object()
        .ok_or_else(|| anyhow!("Messages body must be an object"))?
    {
        if !matches!(
            key.as_str(),
            "model" | "messages" | "thinking" | "output_config"
        ) {
            ensure!(
                prepared.get(key) == Some(value),
                "ARC session changed non-steering field {key}"
            );
        }
    }
    ensure!(
        prepared
            .as_object()
            .is_some_and(|map| map.keys().all(|key| body.get(key).is_some()
                || matches!(key.as_str(), "thinking" | "output_config"))),
        "ARC session added unexpected request fields"
    );
    *body = prepared.clone();
    // Retain the guard through dispatch errors and response-body cancellation.
    turn.settled = false;
    Ok((
        RouteDecision {
            target: RouteSelection::Endpoint(binding.target.endpoint.clone()),
            requested_model: crate::arc::VIRTUAL_MODEL.into(),
            selected_model: binding.target.model.clone(),
            policy: "arc_session".into(),
            task_class: "interactive".into(),
            route_id: operation,
        },
        turn,
    ))
}
struct PrepareGuard {
    config: SessionConfig,
    armed: bool,
}
impl Drop for PrepareGuard {
    fn drop(&mut self) {
        if self.armed {
            self.config.runtime.uncertain.store(true, Ordering::SeqCst);
        }
    }
}

fn settlement_payload(owner: &str, token: &str, output: Option<Value>) -> Value {
    let mut payload = json!({"owner_id":owner,"session_token":token});
    if let Some(output) = output {
        payload["settlement"] = json!("successful_2xx_terminal_sent");
        payload["response_messages"] = output;
    }
    payload
}

pub(crate) struct Turn {
    config: SessionConfig,
    owner: String,
    token: String,
    settled: bool,
    accepted_output: Option<Value>,
    saw_terminal_keyword: bool,
    gate: Option<tokio::sync::OwnedMutexGuard<()>>,
}
impl Turn {
    async fn settle(&mut self, output: Option<Value>) -> Result<()> {
        let success = output.is_some();
        let payload = settlement_payload(&self.owner, &self.token, output);
        self.settled = true;
        let mut pending = PrepareGuard {
            config: self.config.clone(),
            armed: true,
        };
        let result = self
            .config
            .call(if success { "commit" } else { "abort" }, &payload)
            .await;
        let result = result.and_then(|v| {
            ensure!(
                v["state"] == if success { "committed" } else { "aborted" },
                "ARC settlement acknowledgement mismatch"
            );
            Ok(())
        });
        pending.armed = false;
        if result.is_err() {
            self.config.runtime.uncertain.store(true, Ordering::SeqCst);
        }
        result
    }
    pub(crate) fn observe(self, response: Response<BoxBody>) -> Response<BoxBody> {
        let (mut parts, body) = response.into_parts();
        // A fixed Content-Length lets Hyper finish without polling body EOF.
        // Streaming framing keeps settlement inside the consumed body lifecycle.
        parts.headers.remove(hyper::header::CONTENT_LENGTH);
        let successful = parts.status.is_success();
        let sse = parts
            .headers
            .get("content-type")
            .and_then(|h| h.to_str().ok())
            .is_some_and(|s| s.starts_with("text/event-stream"));
        let stream = stream::unfold(Some((body, self, Vec::new())), move |state| async move {
            let (mut body, mut turn, mut data) = state?;
            if let Some(output) = turn.accepted_output.take() {
                return match turn.settle(Some(output)).await {
                    Ok(()) => None,
                    Err(_) => Some((
                        Err(std::io::Error::other("ARC terminal settlement failed")),
                        None,
                    )),
                };
            }
            match body.frame().await {
                Some(Ok(frame)) => {
                    if let Some(bytes) = frame.data_ref() {
                        if data.len().saturating_add(bytes.len()) > turn.config.max_response_bytes {
                            let _ = turn.settle(None).await;
                            return Some((
                                Err(std::io::Error::other(
                                    "ARC response observation limit exceeded",
                                )),
                                None,
                            ));
                        }
                        let old_len = data.len();
                        data.extend_from_slice(bytes);
                        if successful && sse {
                            turn.saw_terminal_keyword |= data[old_len.saturating_sub(32)..]
                                .windows(b"message_stop".len())
                                .any(|w| w == b"message_stop");
                            if turn.saw_terminal_keyword
                                && (data.ends_with(b"\n\n") || data.ends_with(b"\r\n\r\n"))
                            {
                                // This frame is about to be accepted by the body consumer.
                                // A client may stop reading immediately after message_stop.
                                turn.accepted_output = completed_output(&data, true).ok();
                            }
                        }
                    }
                    Some((Ok(frame), Some((body, turn, data))))
                }
                Some(Err(error)) => {
                    let _ = turn.settle(None).await;
                    Some((Err(error), None))
                }
                None => {
                    let output = if successful {
                        completed_output(&data, sse).ok()
                    } else {
                        None
                    };
                    let invalid = successful && output.is_none();
                    let result = turn.settle(output).await;
                    if invalid || result.is_err() {
                        Some((
                            Err(std::io::Error::other(
                                "ARC response incomplete or session settlement failed",
                            )),
                            None,
                        ))
                    } else {
                        None
                    }
                }
            }
        });
        let body: BoxBody = StreamBody::new(stream).boxed();
        Response::from_parts(parts, body)
    }
}
impl Drop for Turn {
    fn drop(&mut self) {
        if !self.settled {
            // Resume only after an acknowledged abort. Unknown settlement blocks
            // this owner permanently; no silent retry against ambiguous state.
            self.config.runtime.aborting.fetch_add(1, Ordering::SeqCst);
            let config = self.config.clone();
            let output = self.accepted_output.take();
            let success = output.is_some();
            let payload = settlement_payload(&self.owner, &self.token, output);
            let gate = self.gate.take();
            if let Ok(runtime) = tokio::runtime::Handle::try_current() {
                runtime.spawn(async move {
                    let _gate = gate;
                    let mut pending = PrepareGuard {
                        config: config.clone(),
                        armed: true,
                    };
                    let result = config
                        .call(if success { "commit" } else { "abort" }, &payload)
                        .await;
                    if !result
                        .is_ok_and(|v| v["state"] == if success { "committed" } else { "aborted" })
                    {
                        config.runtime.uncertain.store(true, Ordering::SeqCst);
                    }
                    config.runtime.aborting.fetch_sub(1, Ordering::SeqCst);
                    pending.armed = false;
                });
            } else {
                self.config.runtime.uncertain.store(true, Ordering::SeqCst);
            }
        }
    }
}

fn completed_output(bytes: &[u8], sse: bool) -> Result<Value> {
    if !sse {
        let response: Value = serde_json::from_slice(bytes)?;
        ensure!(
            response["type"] == "message"
                && response["role"] == "assistant"
                && response["stop_reason"].is_string()
                && response["content"].is_array(),
            "incomplete Messages response"
        );
        return Ok(json!([{"role":"assistant","content":response["content"]}]));
    }
    let text = std::str::from_utf8(bytes)?.replace("\r\n", "\n");
    let mut blocks: Vec<Value> = Vec::new();
    let mut started = false;
    let mut stopped = false;
    let mut closed = true;
    let mut stop_reason = false;
    let mut tool_json = String::new();
    for event in text.split("\n\n").filter(|s| !s.trim().is_empty()) {
        let data = event
            .lines()
            .filter_map(|l| l.strip_prefix("data:").map(str::trim_start))
            .collect::<Vec<_>>()
            .join("\n");
        if data.is_empty() {
            continue;
        }
        let value: Value = serde_json::from_str(&data)?;
        ensure!(!stopped, "event after terminal response");
        match value["type"].as_str() {
            Some("ping") => {}
            Some("message_start") => {
                ensure!(
                    !started
                        && value["message"]["role"] == "assistant"
                        && value["message"]["content"] == json!([]),
                    "invalid message start"
                );
                started = true;
            }
            Some("content_block_start") => {
                ensure!(
                    started && closed && value["index"].as_u64() == Some(blocks.len() as u64),
                    "invalid block start"
                );
                blocks.push(value["content_block"].clone());
                closed = false;
                tool_json.clear();
            }
            Some("content_block_delta") => {
                ensure!(
                    !closed && value["index"].as_u64() == Some((blocks.len() - 1) as u64),
                    "invalid block delta"
                );
                let block = blocks.last_mut().ok_or_else(|| anyhow!("missing block"))?;
                let delta = &value["delta"];
                let pair = match delta["type"].as_str() {
                    Some("text_delta") => Some(("text", "text")),
                    Some("thinking_delta") => Some(("thinking", "thinking")),
                    Some("signature_delta") => Some(("signature", "signature")),
                    Some("input_json_delta") => {
                        tool_json.push_str(
                            delta["partial_json"]
                                .as_str()
                                .ok_or_else(|| anyhow!("invalid tool delta"))?,
                        );
                        None
                    }
                    _ => return Err(anyhow!("unsupported native delta")),
                };
                if let Some((key, field)) = pair {
                    let addition = delta[field]
                        .as_str()
                        .ok_or_else(|| anyhow!("invalid string delta"))?;
                    let mut text = block[key].as_str().unwrap_or("").to_owned();
                    text.push_str(addition);
                    block[key] = Value::String(text);
                }
            }
            Some("content_block_stop") => {
                ensure!(
                    !closed && value["index"].as_u64() == Some((blocks.len() - 1) as u64),
                    "invalid block stop"
                );
                if !tool_json.is_empty() {
                    let block = blocks.last_mut().ok_or_else(|| anyhow!("missing block"))?;
                    block["input"] = serde_json::from_str(&tool_json)?;
                }
                closed = true;
            }
            Some("message_delta") => {
                ensure!(started && closed, "invalid message delta");
                stop_reason = value["delta"]["stop_reason"].is_string();
            }
            Some("message_stop") => {
                ensure!(started && closed && stop_reason, "incomplete response");
                stopped = true;
            }
            _ => return Err(anyhow!("unsupported or failed Messages event")),
        }
    }
    ensure!(stopped, "missing terminal Messages event");
    Ok(json!([{"role":"assistant","content":blocks}]))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use hyper::body::Frame;
    use hyper::service::service_fn;
    use hyper_util::rt::{TokioExecutor, TokioIo};
    use std::sync::Mutex;

    async fn service() -> (
        SessionConfig,
        Arc<Mutex<Vec<(String, Value)>>>,
        tokio::task::JoinHandle<()>,
    ) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let config:SessionConfig=serde_json::from_value(json!({"base_url":format!("http://{}",listener.local_addr().unwrap()),"timeout_ms":2000,"max_response_bytes":65536})).unwrap();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let copy = requests.clone();
        let server = tokio::spawn(async move {
            loop {
                let (socket, _) = listener.accept().await.unwrap();
                let copy = copy.clone();
                tokio::spawn(async move {
                    let _=hyper_util::server::conn::auto::Builder::new(TokioExecutor::new()).serve_connection(TokioIo::new(socket),service_fn(move |req:hyper::Request<hyper::body::Incoming>|{let copy=copy.clone();async move {
            let path=req.uri().path().to_owned();let value:Value=serde_json::from_slice(&req.into_body().collect().await.unwrap().to_bytes()).unwrap();
            copy.lock().unwrap().push((path.clone(),value));
            let result=json!({"state":if path.ends_with("commit"){"committed"}else{"aborted"}});
            Ok::<_,std::convert::Infallible>(Response::new(http_body_util::Full::new(Bytes::from(result.to_string()))))
        }})).await;
                });
            }
        });
        (config, requests, server)
    }
    fn turn(config: SessionConfig) -> Turn {
        Turn {
            config,
            owner: "synthetic-owner".into(),
            token: "synthetic-token".into(),
            settled: false,
            accepted_output: None,
            saw_terminal_keyword: false,
            gate: None,
        }
    }
    fn body(value: Value) -> Response<BoxBody> {
        Response::new(
            http_body_util::Full::new(Bytes::from(value.to_string()))
                .map_err(|never| match never {})
                .boxed(),
        )
    }
    fn complete() -> Value {
        json!({"type":"message","role":"assistant","stop_reason":"tool_use","content":[{"type":"thinking","thinking":"synthetic reasoning","signature":"synthetic signature"},{"type":"tool_use","id":"tool-a","name":"Read","input":{"path":"example"},"caller":{"type":"direct"}}]})
    }

    #[test]
    fn native_identity_requires_real_session_and_retains_lineage() {
        assert!(native_identity(&HeaderMap::new(), &json!({})).is_err());
        let mut headers = HeaderMap::new();
        headers.insert("x-claude-code-agent-id", "child".parse().unwrap());
        let identity = native_identity(
            &headers,
            &json!({"metadata":{"user_id":"{\"session_id\":\"native-session\"}"}}),
        )
        .unwrap();
        assert_eq!(identity["session_id"], "native-session");
        assert_eq!(identity["agent_id"], "child");
    }
    #[tokio::test]
    async fn exact_native_output_commits_only_after_complete_body_consumption() {
        let (config, requests, server) = service().await;
        let response = turn(config).observe(body(complete()));
        assert!(requests.lock().unwrap().is_empty());
        let delivered = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(
            serde_json::from_slice::<Value>(&delivered).unwrap(),
            complete()
        );
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert!(requests[0].0.ends_with("commit"));
        assert_eq!(
            requests[0].1["response_messages"][0]["content"],
            complete()["content"]
        );
        server.abort();
    }
    #[tokio::test]
    async fn missing_terminal_and_upstream_error_abort() {
        let (config, requests, server) = service().await;
        let mut incomplete = complete();
        incomplete.as_object_mut().unwrap().remove("stop_reason");
        assert!(
            turn(config.clone())
                .observe(body(incomplete))
                .into_body()
                .collect()
                .await
                .is_err()
        );
        let mut error = body(complete());
        *error.status_mut() = hyper::StatusCode::BAD_GATEWAY;
        turn(config)
            .observe(error)
            .into_body()
            .collect()
            .await
            .unwrap();
        assert!(
            requests
                .lock()
                .unwrap()
                .iter()
                .all(|(p, _)| p.ends_with("abort"))
        );
        server.abort();
    }
    #[tokio::test]
    async fn drop_aborts_and_acknowledged_abort_allows_retry() {
        let (config, requests, server) = service().await;
        drop(turn(config.clone()).observe(body(complete())));
        assert_eq!(config.runtime.aborting.load(Ordering::SeqCst), 1);
        tokio::time::timeout(Duration::from_secs(2), async {
            while config.runtime.aborting.load(Ordering::SeqCst) != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(!config.runtime.uncertain.load(Ordering::SeqCst));
        assert!(requests.lock().unwrap()[0].0.ends_with("abort"));
        assert!(requests.lock().unwrap()[0].1.get("settlement").is_none());
        assert!(
            requests.lock().unwrap()[0]
                .1
                .get("response_messages")
                .is_none()
        );
        server.abort();
    }
    fn events() -> String {
        [json!({"type":"message_start","message":{"role":"assistant","content":[]}}),json!({"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":"","signature":""}}),json!({"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"reason α"}}),json!({"type":"content_block_delta","index":0,"delta":{"type":"signature_delta","signature":"signed"}}),json!({"type":"content_block_stop","index":0}),json!({"type":"content_block_start","index":1,"content_block":{"type":"tool_use","id":"call-a","name":"Read","input":{},"caller":{"type":"direct"}}}),json!({"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"{\"path\":\"file\"}"}}),json!({"type":"content_block_stop","index":1}),json!({"type":"message_delta","delta":{"stop_reason":"tool_use"}}),json!({"type":"message_stop"})].iter().map(|v|format!("data: {v}\r\n\r\n")).collect()
    }
    #[tokio::test]
    async fn chunked_sse_keeps_signed_thinking_tool_identity_and_wire_unchanged() {
        let (config, requests, server) = service().await;
        let wire = events();
        let chunks = wire
            .as_bytes()
            .chunks(3)
            .map(|s| Ok::<_, std::io::Error>(Frame::data(Bytes::copy_from_slice(s))))
            .collect::<Vec<_>>();
        let mut response = Response::new(StreamBody::new(stream::iter(chunks)).boxed());
        response
            .headers_mut()
            .insert("content-type", "text/event-stream".parse().unwrap());
        let data = turn(config)
            .observe(response)
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes();
        assert_eq!(data.as_ref(), wire.as_bytes());
        let requests = requests.lock().unwrap();
        let content = &requests[0].1["response_messages"][0]["content"];
        assert_eq!(
            content[0],
            json!({"type":"thinking","thinking":"reason α","signature":"signed"})
        );
        assert_eq!(content[1]["caller"], json!({"type":"direct"}));
        assert_eq!(content[1]["input"], json!({"path":"file"}));
        server.abort();
    }
    #[test]
    fn truncated_error_and_unknown_events_never_commit() {
        let wire = events();
        assert!(completed_output(wire.replace("message_stop", "error").as_bytes(), true).is_err());
        assert!(
            completed_output(
                wire.split("data: {\"type\":\"message_stop\"}")
                    .next()
                    .unwrap()
                    .as_bytes(),
                true
            )
            .is_err()
        );
        assert!(
            completed_output(
                format!("{wire}data: {{\"type\":\"ping\"}}\n\n").as_bytes(),
                true
            )
            .is_err()
        );
    }
    async fn mock_json(
        handler: Arc<dyn Fn(String, Value) -> Value + Send + Sync>,
    ) -> (String, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            loop {
                let (socket, _) = listener.accept().await.unwrap();
                let handler = handler.clone();
                tokio::spawn(async move {
                    let _ = hyper_util::server::conn::auto::Builder::new(TokioExecutor::new())
                        .serve_connection(
                            TokioIo::new(socket),
                            service_fn(move |req: hyper::Request<hyper::body::Incoming>| {
                                let handler = handler.clone();
                                async move {
                                    let path = req.uri().path().to_owned();
                                    let value = serde_json::from_slice(
                                        &req.into_body().collect().await.unwrap().to_bytes(),
                                    )
                                    .unwrap();
                                    Ok::<_, std::convert::Infallible>(Response::new(
                                        http_body_util::Full::new(Bytes::from(
                                            handler(path, value).to_string(),
                                        )),
                                    ))
                                }
                            }),
                        )
                        .await;
                });
            }
        });
        (format!("http://{address}"), task)
    }
    #[tokio::test]
    async fn two_native_http_turns_compose_prepare_private_tool_tail_dispatch_and_commit() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let seen = requests.clone();
        let (service_url,service_task)=mock_json(Arc::new(move |path,input| {
            seen.lock().unwrap().push((path.clone(),input.clone()));
            if !path.ends_with("prepare") {return json!({"state":if path.ends_with("commit"){"committed"}else{"aborted"}});}
            let mut provider=input["request"].clone();provider["model"]=json!("claude-sonnet-5");
            // This fixed native control deliberately differs from the ordinary
            // model-name adaptation; session dispatch must preserve it exactly.
            provider["thinking"]=json!({"type":"enabled","budget_tokens":2048});
            let last=provider["messages"].as_array_mut().unwrap().last_mut().unwrap();
            if let Some(text)=last["content"].as_str(){last["content"]=json!([{"type":"text","text":text}]);}
            last["content"].as_array_mut().unwrap().push(json!({"type":"text","text":"synthetic private tail"}));
            json!({"owner_id":input["owner_id"],"session_token":input["operation_id"],"package_sha256":"a".repeat(64),"request_format":"anthropic_messages","source_request_format":if input["request"]["max_tokens"]==17 {"openai_chat"}else{"anthropic_messages"},"action_id":"synthetic","request":provider,"decision":{"schema_version":"rayline.arc.policy-decision-response.v1","package":{"alias":"synthetic","package_sha256":"a".repeat(64)},"decision":{"selected_action_id":"synthetic","selected_arm_id":"b".repeat(64)},"encoding":{"session_revision":1},"actions":[{"action_id":"synthetic","available":true,"selected":true}]}})
        })).await;
        let provider_requests = Arc::new(Mutex::new(Vec::new()));
        let seen = provider_requests.clone();
        let output = complete();
        let returned = output.clone();
        let (provider_url, provider_task) = mock_json(Arc::new(move |_, input| {
            seen.lock().unwrap().push(input);
            returned.clone()
        }))
        .await;
        let config:crate::RouterConfig=serde_json::from_value(json!({"endpoints":[{"id":"provider","protocol":"anthropic_messages","base_url":provider_url}],"arc":{"base_url":"http://127.0.0.1:1","package_alias":"synthetic","package_sha256":"a".repeat(64),"timeout_ms":2000,"session":{"base_url":service_url,"timeout_ms":2000,"max_response_bytes":65536},"bindings":{"synthetic":{"target":{"endpoint":"provider","model":"claude-sonnet-5"},"request_overrides":{"thinking":{"type":"enabled","budget_tokens":2048}}}}}})).unwrap();
        config.arc.as_ref().unwrap().validate(&config).unwrap();
        let state = AppState {
            opts: Arc::new(crate::LocalRouterOptions::default()),
            config: Arc::new(config),
            http: reqwest::Client::new(),
            http_ipv4: reqwest::Client::new(),
            route_counter: Arc::new(std::sync::atomic::AtomicU64::new(1)),
            started_at: "synthetic".into(),
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let router = tokio::spawn(async move {
            loop {
                let (socket, _) = listener.accept().await.unwrap();
                let state = state.clone();
                tokio::spawn(async move {
                    let _ = hyper_util::server::conn::auto::Builder::new(TokioExecutor::new())
                        .serve_connection(
                            TokioIo::new(socket),
                            service_fn(move |request| {
                                let state = state.clone();
                                async move {
                                    Ok::<_, std::convert::Infallible>(
                                        crate::handle(state, request).await,
                                    )
                                }
                            }),
                        )
                        .await;
                });
            }
        });
        let mut messages = json!([{"role":"user","content":"read synthetic file"}]);
        for turn_index in 0..2 {
            let request = json!({"model":"rayline-arc","max_tokens":16,"metadata":{"user_id":"{\"session_id\":\"synthetic-native\"}"},"messages":messages});
            let response = reqwest::Client::new()
                .post(format!("http://{address}/v1/messages"))
                .json(&request)
                .send()
                .await
                .unwrap();
            assert!(response.status().is_success());
            let response: Value = response.json().await.unwrap();
            assert_eq!(response, output);
            tokio::time::timeout(Duration::from_secs(2), async {
                loop {
                    if requests
                        .lock()
                        .unwrap()
                        .iter()
                        .filter(|(p, _)| p.ends_with("commit"))
                        .count()
                        == turn_index + 1
                    {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            messages.as_array_mut().unwrap().extend([json!({"role":"assistant","content":response["content"]}),json!({"role":"user","content":[{"type":"tool_result","tool_use_id":"tool-a","content":"synthetic result","cache_control":{"type":"ephemeral"}}]})]);
        }
        let invalid = json!({"model":"rayline-arc","max_tokens":17,"metadata":{"user_id":"{\"session_id\":\"synthetic-native\"}"},"messages":messages});
        let response = reqwest::Client::new()
            .post(format!("http://{address}/v1/messages"))
            .json(&invalid)
            .send()
            .await
            .unwrap();
        assert!(!response.status().is_success());
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if requests
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|(p, _)| p.ends_with("abort"))
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 6);
        assert_eq!(requests[0].1["owner_id"], requests[2].1["owner_id"]);
        assert_ne!(requests[0].1["operation_id"], requests[2].1["operation_id"]);
        assert_eq!(requests[0].1["metadata"]["session_id"], "synthetic-native");
        assert!(
            !requests[2].1["request"]
                .to_string()
                .contains("synthetic private tail")
        );
        assert_eq!(
            requests[1].1["response_messages"][0]["content"],
            output["content"]
        );
        let provider = provider_requests.lock().unwrap();
        assert_eq!(provider.len(), 2);
        assert_eq!(
            provider[0]["thinking"],
            json!({"type":"enabled","budget_tokens":2048})
        );
        assert!(provider[0].get("output_config").is_none());
        let tail = &provider[1]["messages"][2]["content"];
        assert_eq!(tail[0]["type"], "tool_result");
        assert_eq!(tail[0]["cache_control"], json!({"type":"ephemeral"}));
        assert_eq!(tail[1]["text"], "synthetic private tail");
        router.abort();
        provider_task.abort();
        service_task.abort();
    }
    #[tokio::test]
    async fn terminal_frame_then_drop_commits_without_waiting_for_eof() {
        let (config, requests, server) = service().await;
        let frames =
            stream::once(async { Ok::<_, std::io::Error>(Frame::data(Bytes::from(events()))) });
        use futures::StreamExt;
        let never = stream::pending::<std::io::Result<Frame<Bytes>>>();
        let mut response = Response::new(BodyExt::boxed(StreamBody::new(frames.chain(never))));
        response
            .headers_mut()
            .insert("content-type", "text/event-stream".parse().unwrap());
        let gate = Arc::new(tokio::sync::Mutex::new(()));
        let mut transaction = turn(config.clone());
        transaction.gate = Some(gate.clone().lock_owned().await);
        let mut body = transaction.observe(response).into_body();
        body.frame().await.unwrap().unwrap();
        drop(body);
        assert!(
            gate.try_lock().is_err(),
            "continuation must wait for terminal settlement acknowledgement"
        );
        tokio::time::timeout(Duration::from_secs(2), async {
            while config.runtime.aborting.load(Ordering::SeqCst) != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(gate.try_lock().is_ok());
        assert_eq!(requests.lock().unwrap().len(), 1);
        assert!(requests.lock().unwrap()[0].0.ends_with("commit"));
        server.abort();
    }
    #[tokio::test]
    async fn session_disconnect_cancels_a_stalled_provider_body() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (tx, rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let mut chunk = [0u8; 1024];
            while !request.windows(4).any(|w| w == b"\r\n\r\n") {
                let n = socket.read(&mut chunk).await.unwrap();
                assert!(n > 0);
                request.extend_from_slice(&chunk[..n]);
            }
            socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n").await.unwrap();
            let n = socket.read(&mut chunk).await.unwrap();
            tx.send(n).unwrap();
        });
        let upstream = reqwest::Client::new()
            .get(format!("http://{address}"))
            .send()
            .await
            .unwrap();
        let decision = RouteDecision {
            target: RouteSelection::Endpoint("fake".into()),
            requested_model: "rayline-arc".into(),
            selected_model: "fake".into(),
            policy: "arc_session".into(),
            task_class: "interactive".into(),
            route_id: "synthetic".into(),
        };
        let response = crate::response_from_reqwest(
            upstream,
            reqwest::StatusCode::OK,
            Some(&decision),
            None,
            None,
            None,
        )
        .await
        .unwrap();
        drop(response);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), rx)
                .await
                .unwrap()
                .unwrap(),
            0
        );
        server.await.unwrap();
    }
}
