//! Experimental ARC adapter. The worker owns inference; callers own episode state.
use std::collections::HashMap;
use std::net::IpAddr;
use std::time::Duration;

use anyhow::{Result, anyhow, ensure};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{AppState, RouteDecision, RouteSelection, RouteTarget, RouterConfig};

pub const VIRTUAL_MODEL: &str = "rayline-arc";
const PATH: &str = "/v1/rayline/arc/policy/decide";
const REQUEST_SCHEMA: &str = "rayline.arc.policy-decision-request.v1";
const RESPONSE_SCHEMA: &str = "rayline.arc.policy-decision-response.v1";

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArcBinding {
    pub target: RouteTarget,
    /// Native worker controls in the endpoint's format. Empty is explicit.
    pub request_overrides: serde_json::Map<String, Value>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArcConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session: Option<crate::arc_session::SessionConfig>,
    pub base_url: String,
    pub package_alias: String,
    pub package_sha256: String,
    pub timeout_ms: u64,
    pub bindings: HashMap<String, ArcBinding>,
}

impl ArcConfig {
    fn url(&self) -> Result<reqwest::Url> {
        let url = reqwest::Url::parse(&self.base_url)?;
        let local = url
            .host_str()
            .and_then(|host| host.trim_matches(['[', ']']).parse::<IpAddr>().ok())
            .is_some_and(|ip| ip.is_loopback());
        ensure!(
            url.scheme() == "http"
                && local
                && url.username().is_empty()
                && url.password().is_none()
                && url.query().is_none()
                && url.fragment().is_none()
                && url.path() == "/",
            "ARC worker must be an HTTP literal loopback origin"
        );
        Ok(url.join(PATH)?)
    }

    pub(crate) fn validate(&self, config: &RouterConfig) -> Result<()> {
        self.url()?;
        if let Some(session) = &self.session {
            session.validate(self)?;
        }
        ensure!(self.timeout_ms > 0, "ARC timeout must be positive");
        ensure!(
            !self.package_alias.is_empty()
                && self.package_sha256.len() == 64
                && self.package_sha256.bytes().all(|c| c.is_ascii_hexdigit()),
            "ARC package identity is required"
        );
        ensure!(
            !self.bindings.is_empty(),
            "ARC action bindings are required"
        );
        for (action, binding) in &self.bindings {
            ensure!(
                !action.is_empty() && !binding.target.model.is_empty(),
                "ARC action and model must be explicit"
            );
            // Local redirects cannot apply per-action request mutations yet.
            ensure!(
                binding.target.endpoint != "local",
                "ARC requires a configured forwarding endpoint, not a local redirect"
            );
            ensure!(
                config
                    .endpoints
                    .iter()
                    .any(|endpoint| endpoint.id == binding.target.endpoint
                        && (endpoint.protocol == crate::EndpointProtocol::AnthropicMessages
                            || (endpoint.protocol == crate::EndpointProtocol::OpenAIChat
                                && self
                                    .session
                                    .as_ref()
                                    .is_some_and(|s| s.codec_sha256.is_some())))),
                "ARC Chat endpoints require opt-in session codec; otherwise native Messages required"
            );
            let chat = config.endpoints.iter().any(|endpoint| {
                endpoint.id == binding.target.endpoint
                    && endpoint.protocol == crate::EndpointProtocol::OpenAIChat
            });
            ensure!(
                binding.request_overrides.keys().all(|key| if chat {
                    matches!(
                        key.as_str(),
                        "reasoning_effort" | "reasoning" | "chat_template_kwargs"
                    )
                } else {
                    matches!(key.as_str(), "thinking" | "output_config")
                }),
                "ARC worker controls must match its configured provider format"
            );
        }
        Ok(())
    }

    /// Send an exact existing ARC decide envelope. Never forwards inbound credentials.
    pub async fn decide(&self, envelope: &Value) -> Result<Value> {
        ensure!(
            envelope["schema_version"] == REQUEST_SCHEMA,
            "ARC request schema mismatch"
        );
        ensure!(
            envelope["package"]["alias"] == self.package_alias
                && envelope["package"]["package_sha256"] == self.package_sha256,
            "ARC request package mismatch"
        );
        for field in [
            "episode_id_hash",
            "context_epoch",
            "request_format",
            "request",
            "attribution",
            "selection",
        ] {
            ensure!(envelope.get(field).is_some(), "ARC context missing {field}");
        }
        let offered = envelope["selection"]["available_action_ids"]
            .as_array()
            .ok_or_else(|| anyhow!("ARC available_action_ids must be explicit"))?;
        ensure!(
            !offered.is_empty()
                && offered.iter().all(|action| action
                    .as_str()
                    .is_some_and(|id| self.bindings.contains_key(id))),
            "ARC offered actions must all have configured bindings"
        );
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .timeout(Duration::from_millis(self.timeout_ms))
            .build()?;
        let response = client.post(self.url()?).json(envelope).send().await?;
        ensure!(
            response.status().is_success(),
            "ARC worker returned HTTP {}",
            response.status()
        );
        let result: Value = response.json().await?;
        self.validate_response(envelope, &result)?;
        Ok(result)
    }

    pub(crate) fn validate_response(&self, envelope: &Value, result: &Value) -> Result<()> {
        ensure!(
            result["schema_version"] == RESPONSE_SCHEMA,
            "ARC response schema mismatch"
        );
        ensure!(
            result["package"]["alias"] == self.package_alias
                && result["package"]["package_sha256"] == self.package_sha256,
            "ARC response package mismatch"
        );
        let selected = result["decision"]["selected_action_id"]
            .as_str()
            .ok_or_else(|| anyhow!("ARC selected action missing"))?;
        ensure!(
            self.bindings.contains_key(selected),
            "ARC selected action has no binding"
        );
        ensure!(
            envelope["selection"]["available_action_ids"]
                .as_array()
                .is_some_and(|actions| actions.iter().any(|action| action == selected)),
            "ARC selected unavailable action"
        );
        if let Some(held) = envelope["selection"]["held_action_id"].as_str() {
            ensure!(
                held == selected,
                "ARC selected action differs from held action"
            );
        }
        ensure!(
            result["decision"]["selected_arm_id"]
                .as_str()
                .is_some_and(|s| !s.is_empty()),
            "ARC selected arm missing"
        );
        ensure!(
            result["actions"]
                .as_array()
                .is_some_and(|actions| actions
                    .iter()
                    .any(|action| action["action_id"] == selected
                        && action["available"] == true
                        && action["selected"] == true)),
            "ARC selected action eligibility is inconsistent"
        );
        Ok(())
    }
}

pub(crate) async fn route(state: &AppState, body: &mut Value) -> Result<RouteDecision> {
    let config = state
        .config
        .arc
        .as_ref()
        .ok_or_else(|| anyhow!("ARC is not configured"))?;
    let object = body
        .as_object_mut()
        .ok_or_else(|| anyhow!("ARC request must be an object"))?;
    let mut context = object.remove("rayline_arc").ok_or_else(|| anyhow!("explicit rayline_arc context is required; automatic episode tracking is not implemented"))?;
    ensure!(
        context.get("request").is_none(),
        "ARC context must not override the incoming request"
    );
    ensure!(
        context["request_format"] == "anthropic_messages",
        "ARC request format must be anthropic_messages"
    );
    let projected = ["system", "tools", "messages"]
        .into_iter()
        .filter_map(|key| body.get(key).map(|value| (key.to_owned(), value.clone())))
        .collect::<serde_json::Map<String, Value>>();
    context
        .as_object_mut()
        .ok_or_else(|| anyhow!("ARC context must be an object"))?
        .insert("request".to_owned(), Value::Object(projected));
    let result = config.decide(&context).await?;
    let selected = result["decision"]["selected_action_id"]
        .as_str()
        .ok_or_else(|| anyhow!("ARC selected action missing"))?;
    let binding = config
        .bindings
        .get(selected)
        .ok_or_else(|| anyhow!("ARC binding missing"))?;
    ensure!(
        state
            .config
            .endpoints
            .iter()
            .any(|e| e.id == binding.target.endpoint
                && e.protocol == crate::EndpointProtocol::AnthropicMessages),
        "ARC replay supports only native Messages endpoints"
    );
    let object = body
        .as_object_mut()
        .ok_or_else(|| anyhow!("ARC request must be an object"))?;
    // Thinking is part of the selected action, never inherited from the virtual request.
    object.remove("thinking");
    object.remove("output_config");
    object.extend(binding.request_overrides.clone());
    Ok(RouteDecision {
        target: RouteSelection::Endpoint(binding.target.endpoint.clone()),
        requested_model: VIRTUAL_MODEL.to_owned(),
        selected_model: binding.target.model.clone(),
        policy: "arc_explicit_context".to_owned(),
        task_class: "explicit_context".to_owned(),
        route_id: crate::new_request_id(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn worker_origin_rejects_remote_and_ambiguous_urls() {
        for origin in [
            "http://localhost:8000",
            "https://127.0.0.1",
            "http://example.com",
            "http://127.0.0.1/path",
            "http://user@127.0.0.1",
        ] {
            let config = ArcConfig {
                session: None,
                base_url: origin.into(),
                package_alias: "test".into(),
                package_sha256: "a".repeat(64),
                timeout_ms: 1000,
                bindings: HashMap::new(),
            };
            assert!(config.url().is_err(), "{origin}");
        }
    }
    async fn reply_once(response: Value) -> (String, tokio::sync::oneshot::Receiver<Value>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (tx, rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut bytes = Vec::new();
            let request = loop {
                let mut buffer = [0; 4096];
                let count = socket.read(&mut buffer).await.unwrap();
                assert!(count > 0);
                bytes.extend_from_slice(&buffer[..count]);
                if let Some(end) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                    let headers = String::from_utf8_lossy(&bytes[..end]);
                    let length: usize = headers
                        .lines()
                        .find_map(|line| {
                            line.to_ascii_lowercase()
                                .strip_prefix("content-length: ")
                                .map(str::to_owned)
                        })
                        .unwrap()
                        .parse()
                        .unwrap();
                    if bytes.len() >= end + 4 + length {
                        break serde_json::from_slice(&bytes[end + 4..end + 4 + length]).unwrap();
                    }
                }
            };
            tx.send(request).unwrap();
            let payload = response.to_string();
            socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{payload}", payload.len()).as_bytes()).await.unwrap();
        });
        (format!("http://{address}"), rx)
    }

    fn setup(base_url: String) -> (ArcConfig, Value, Value) {
        use serde_json::json;
        let config = ArcConfig {
            session: None,
            base_url,
            package_alias: "synthetic".into(),
            package_sha256: "a".repeat(64),
            timeout_ms: 2000,
            bindings: HashMap::from([(
                "action-a".into(),
                ArcBinding {
                    target: RouteTarget {
                        endpoint: "fake".into(),
                        model: "selected-model".into(),
                        ..Default::default()
                    },
                    request_overrides: serde_json::from_value(
                        json!({"thinking":{"type":"disabled"}}),
                    )
                    .unwrap(),
                },
            )]),
        };
        let package =
            json!({"alias": config.package_alias, "package_sha256": config.package_sha256});
        let request = json!({"schema_version":REQUEST_SCHEMA,"package":package,"episode_id_hash":"b".repeat(64),"context_epoch":"first","request_format":"anthropic_messages","request":{"messages":[{"role":"user","content":"synthetic test"}]},"attribution":[],"selection":{"available_action_ids":["action-a"]}});
        let response = json!({"schema_version":RESPONSE_SCHEMA,"package":package,"decision":{"selected_action_id":"action-a","selected_arm_id":"arm-a"},"actions":[{"action_id":"action-a","available":true,"selected":true}]});
        (config, request, response)
    }

    #[tokio::test]
    async fn exact_envelope_and_fail_closed_response_validation() {
        let (mut config, request, response) = setup("http://127.0.0.1:1".into());
        let (url, observed) = reply_once(response.clone()).await;
        config.base_url = url;
        assert_eq!(config.decide(&request).await.unwrap(), response);
        assert_eq!(observed.await.unwrap(), request);
        for pointer in [
            "/schema_version",
            "/package/package_sha256",
            "/decision/selected_action_id",
            "/decision/selected_arm_id",
        ] {
            let mut bad = response.clone();
            *bad.pointer_mut(pointer).unwrap() = Value::String(String::new());
            assert!(config.validate_response(&request, &bad).is_err());
        }
        let mut bad = response;
        bad["actions"][0]["available"] = Value::Bool(false);
        assert!(config.validate_response(&request, &bad).is_err());
    }

    #[tokio::test]
    async fn messages_request_uses_arc_then_dispatches_bound_action() {
        use hyper::service::service_fn;
        use hyper_util::rt::{TokioExecutor, TokioIo};
        use serde_json::json;
        use std::sync::{Arc, atomic::AtomicU64};
        let (mut arc, envelope, response) = setup("http://127.0.0.1:1".into());
        let (worker_url, worker_request) = reply_once(response).await;
        arc.base_url = worker_url;
        let (provider_url, provider_request) = reply_once(json!({"id":"synthetic-response","type":"message","role":"assistant","model":"selected-model","content":[{"type":"text","text":"ok"}],"usage":{"input_tokens":1,"output_tokens":1}})).await;
        let config = RouterConfig {
            arc: Some(arc),
            endpoints: vec![crate::EndpointConfig {
                id: "fake".into(),
                kind: "provider".into(),
                protocol: crate::EndpointProtocol::AnthropicMessages,
                base_url: provider_url,
                api_key_env: None,
                models: vec![],
                headers: HashMap::new(),
                auth: None,
            }],
            ..Default::default()
        };
        config.arc.as_ref().unwrap().validate(&config).unwrap();
        let state = AppState {
            opts: Arc::new(crate::LocalRouterOptions::default()),
            config: Arc::new(config),
            http: reqwest::Client::new(),
            http_ipv4: reqwest::Client::new(),
            route_counter: Arc::new(AtomicU64::new(1)),
            started_at: "test".into(),
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            hyper_util::server::conn::auto::Builder::new(TokioExecutor::new())
                .serve_connection(
                    TokioIo::new(socket),
                    service_fn(move |request| {
                        let state = state.clone();
                        async move {
                            Ok::<_, std::convert::Infallible>(crate::handle(state, request).await)
                        }
                    }),
                )
                .await
                .unwrap();
        });
        let mut context = envelope.clone();
        context.as_object_mut().unwrap().remove("request");
        let request = json!({"model":VIRTUAL_MODEL,"messages":[{"role":"user","content":"synthetic test"}],"max_tokens":8,"rayline_arc":context,"thinking":{"type":"enabled","budget_tokens":1000}});
        let result = reqwest::Client::new()
            .post(format!("http://{address}/v1/messages"))
            .json(&request)
            .send()
            .await
            .unwrap();
        assert_eq!(
            result.status(),
            reqwest::StatusCode::OK,
            "{}",
            result.text().await.unwrap()
        );
        let observed = provider_request.await.unwrap();
        assert_eq!(observed["model"], "selected-model");
        assert_eq!(observed["thinking"], json!({"type":"disabled"}));
        assert!(observed.get("rayline_arc").is_none());
        let scored = worker_request.await.unwrap();
        assert_eq!(scored["attribution"], envelope["attribution"]);
        assert_eq!(scored["request"], json!({"messages":request["messages"]}));
        assert_eq!(observed["max_tokens"], 8);
        server.abort();
    }
}
