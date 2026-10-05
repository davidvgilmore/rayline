use super::*;
use futures::StreamExt;
use hyper::{Request, body::Incoming, service::service_fn};
use hyper_util::rt::{TokioExecutor, TokioIo};
use std::sync::{Arc, Mutex};
use std::time::Duration;

const PIN: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
type Log = Arc<Mutex<Vec<(String, Value, hyper::HeaderMap)>>>;
struct Host {
    url: String,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Host {
    fn drop(&mut self) {
        self.task.abort();
    }
}
fn json_response(value: Value) -> Response<BoxBody> {
    Response::builder()
        .header("content-type", "application/json")
        .body(
            Full::new(Bytes::from(value.to_string()))
                .map_err(|never| match never {})
                .boxed(),
        )
        .unwrap()
}
async fn server(
    handler: Arc<dyn Fn(String, Value, hyper::HeaderMap) -> Response<BoxBody> + Send + Sync>,
) -> Host {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        loop {
            let (socket, _) = listener.accept().await.unwrap();
            let handler = handler.clone();
            tokio::spawn(async move {
                let _ = hyper_util::server::conn::auto::Builder::new(TokioExecutor::new())
                    .serve_connection(
                        TokioIo::new(socket),
                        service_fn(move |req: Request<Incoming>| {
                            let handler = handler.clone();
                            async move {
                                let path = req.uri().path().to_owned();
                                let headers = req.headers().clone();
                                let bytes = req.into_body().collect().await.unwrap().to_bytes();
                                Ok::<_, std::convert::Infallible>(handler(
                                    path,
                                    serde_json::from_slice(&bytes).unwrap(),
                                    headers,
                                ))
                            }
                        }),
                    )
                    .await;
            });
        }
    });
    Host { url, task }
}
fn native() -> Value {
    json!({"id":"synthetic","type":"message","role":"assistant","model":"synthetic-chat","stop_reason":"end_turn","content":[{"type":"text","text":"synthetic answer"}]})
}
fn frame(value: Value) -> String {
    format!(
        "event: {}\ndata: {}\n\n",
        value["type"].as_str().unwrap(),
        value
    )
}
fn native_frames() -> Vec<String> {
    vec![
        frame(json!({"type":"message_start","message":{"role":"assistant","content":[]}})),
        frame(
            json!({"type":"content_block_start","index":0,"content_block":{"type":"text","text":"synthetic answer"}}),
        ),
        frame(json!({"type":"content_block_stop","index":0})),
        frame(json!({"type":"message_delta","delta":{"stop_reason":"end_turn"}})),
        frame(json!({"type":"message_stop"})),
    ]
}
struct Fixture {
    host: Host,
    _service: Host,
    _provider: Host,
    log: Log,
    mode: Arc<Mutex<String>>,
    _release: Arc<tokio::sync::Notify>,
}
impl Fixture {
    async fn new() -> Self {
        let log: Log = Arc::default();
        let mode = Arc::new(Mutex::new(String::new()));
        let release = Arc::new(tokio::sync::Notify::new());
        let seen = log.clone();
        let behavior = mode.clone();
        let service=server(Arc::new(move |path,body,headers| {
            seen.lock().unwrap().push((path.clone(),body.clone(),headers));
            let mode=behavior.lock().unwrap().clone();
            if path.ends_with("prepare") {
                let mut messages=body["request"]["messages"].clone();
                messages.as_array_mut().unwrap().push(json!({"role":"user","content":"synthetic private tail"}));
                let mut response=json!({"owner_id":body["owner_id"],"session_token":body["operation_id"],"package_sha256":"a".repeat(64),"request_format":"openai_chat","source_request_format":"anthropic_messages","action_id":"synthetic","request":{"model":"synthetic-chat","messages":messages,"stream":body["request"]["stream"],"reasoning_effort":"high"},"response_codec":{"schema_version":"rayline.arc.response-codec.v1","source":"openai_chat","target":"anthropic_messages","implementation_sha256":PIN},"decision":{"package":{"alias":"synthetic","package_sha256":"a".repeat(64)},"schema_version":"rayline.arc.policy-decision-response.v1","decision":{"selected_action_id":"synthetic","selected_arm_id":"b".repeat(64)},"encoding":{"session_revision":1},"actions":[{"action_id":"synthetic","available":true,"selected":true}]}});
                match mode.as_str() {
                    "bad-pin" => response["response_codec"]["implementation_sha256"]=json!("b".repeat(64)),
                    "bad-source" => response["source_request_format"]=json!("openai_chat"),
                    "bad-controls" => response["request"]["reasoning_effort"]=json!("low"),
                    "bad-model" => response["request"]["model"]=json!("wrong"),
                    _=>{},
                }
                return json_response(response);
            }
            if path.ends_with("codec") {
                let mut response=json!({"implementation_sha256":PIN,"frames":[]});
                if body["operation"]=="stream_start" {response.as_object_mut().unwrap().remove("frames");}
                if mode=="reply-pin" {response["implementation_sha256"]=json!("b".repeat(64));}
                if body["operation"]=="response" {response["body"]=native(); if mode=="oversized-native" { response["body"]["content"][0]["text"]=json!("x".repeat(65537)); }}
                if body["operation"]=="stream_push" && body["frame"].as_str().unwrap().contains("finish_reason") { response["frames"]=json!(native_frames()); if mode=="oversized-stream" { response["frames"][1]=json!(frame(json!({"type":"content_block_start","index":0,"content_block":{"type":"text","text":"x".repeat(65537)}}))); } }
                return json_response(response);
            }
            json_response(json!({"state":if mode=="bad-ack" {"wrong"} else if path.ends_with("commit") {"committed"} else {"aborted"}}))
        })).await;
        let seen = log.clone();
        let behavior = mode.clone();
        let unstall = release.clone();
        let provider=server(Arc::new(move |path,body,headers| {
            seen.lock().unwrap().push((path,body.clone(),headers));let mode=behavior.lock().unwrap().clone();
            if body["stream"]!=true { return json_response(json!({"choices":[{"finish_reason":"stop","message":{"role":"assistant","content":"synthetic answer"}}]})); }
            let chunk="data: {\"choices\":[{\"delta\":{\"content\":\"synthetic answer\"},\"finish_reason\":\"stop\"}]}\n\n".to_owned();
            if mode == "mixed-delimiters" || mode == "bytewise" {
                let text = format!("{}data: [DONE]\n\n", chunk.replace("\n", "\r\n"));
                let chunks: Vec<_> = if mode == "bytewise" { text.into_bytes().into_iter().map(|b| Bytes::from(vec![b])).collect() } else { vec![Bytes::from(text)] };
                return Response::builder().header("content-type","text/event-stream").body(BodyExt::boxed(StreamBody::new(stream::iter(chunks.into_iter().map(|bytes|Ok::<_,std::io::Error>(Frame::data(bytes))))))).unwrap();
            }
            let release=unstall.clone();let blocked=mode=="delay-done";
            let frames=stream::once(async move {Ok::<_,std::io::Error>(Frame::data(Bytes::from(chunk)))}).chain(stream::once(async move {
                if blocked {release.notified().await;}
                Ok(Frame::data(Bytes::from(if mode=="truncated" {"data: [DONE]"} else {"data: [DONE]\n\n"})))
            }));
            Response::builder().header("content-type","text/event-stream").body(BodyExt::boxed(StreamBody::new(frames))).unwrap()
        })).await;
        let config:crate::RouterConfig=serde_json::from_value(json!({"endpoints":[{"id":"provider","protocol":"openai_chat","base_url":format!("{}/v1",provider.url),"headers":{"authorization":"Bearer synthetic-only"}}],"arc":{"base_url":"http://127.0.0.1:1","package_alias":"synthetic","package_sha256":"a".repeat(64),"timeout_ms":2000,"session":{"base_url":service.url,"timeout_ms":2000,"max_response_bytes":65536,"codec_sha256":PIN},"bindings":{"synthetic":{"target":{"endpoint":"provider","model":"synthetic-chat"},"request_overrides":{"reasoning_effort":"high"}}}}})).unwrap();
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
        let url = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
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
        Self {
            host: Host { url, task },
            _service: service,
            _provider: provider,
            log,
            mode,
            _release: release,
        }
    }
    async fn send(&self, streaming: bool, messages: Value) -> reqwest::Response {
        reqwest::Client::new().post(format!("{}/v1/messages",self.host.url)).header("x-client-session-id","synthetic-session").header("authorization","Bearer inbound-not-forwarded").json(&json!({"model":"rayline-arc","stream":streaming,"messages":messages,"max_tokens":32})).send().await.unwrap()
    }
    async fn settled(&self, kind: &str) {
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if self
                    .log
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|(path, _, _)| path.ends_with(kind))
                {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
}
fn messages() -> Value {
    json!([{"role":"user","content":"synthetic task"}])
}
#[tokio::test]
async fn prepared_chat_two_turns_keep_wire_baseline_private_tail_and_host_auth() {
    let f = Fixture::new().await;
    let mut history = messages();
    for _ in 0..2 {
        let response = f.send(false, history.clone()).await;
        assert!(response.status().is_success());
        let output: Value = response.json().await.unwrap();
        assert_eq!(output, native());
        history
            .as_array_mut()
            .unwrap()
            .push(json!({"role":"assistant","content":output["content"]}));
        history
            .as_array_mut()
            .unwrap()
            .push(json!({"role":"user","content":"next task"}));
    }
    f.settled("commit").await;
    let log = f.log.lock().unwrap();
    let providers: Vec<_> = log
        .iter()
        .filter(|(p, _, _)| p == "/v1/chat/completions")
        .collect();
    assert_eq!(providers.len(), 2);
    for (_, body, headers) in providers {
        assert_eq!(body["reasoning_effort"], "high");
        assert_eq!(headers["authorization"], "Bearer synthetic-only");
        assert_eq!(
            body["messages"].as_array().unwrap().last().unwrap()["content"],
            "synthetic private tail"
        );
    }
    for (path, body, headers) in log.iter().filter(|(p, _, _)| p.contains("/experimental/")) {
        assert!(!headers.contains_key("authorization"));
        if path.ends_with("prepare") {
            assert!(
                !body["request"]
                    .to_string()
                    .contains("synthetic private tail")
            );
        }
        if path.ends_with("commit") {
            assert_eq!(
                body["response_messages"],
                json!([{"role":"assistant","content":native()["content"]}])
            );
        }
    }
}
#[tokio::test]
async fn codec_receipt_or_reply_mismatch_never_commits() {
    for mode in [
        "bad-pin",
        "bad-source",
        "bad-controls",
        "bad-model",
        "reply-pin",
        "oversized-native",
    ] {
        let f = Fixture::new().await;
        *f.mode.lock().unwrap() = mode.into();
        let response = f.send(false, messages()).await;
        assert!(!response.status().is_success());
        let _ = response.bytes().await;
        f.settled("abort").await;
        let log = f.log.lock().unwrap();
        assert!(!log.iter().any(|(p, _, _)| p.ends_with("commit")));
        if mode != "reply-pin" && mode != "oversized-native" {
            assert!(!log.iter().any(|(p, _, _)| p.ends_with("chat/completions")));
        }
    }
}
#[tokio::test]
async fn native_terminal_waits_for_provider_done_and_drop_before_done_aborts() {
    let f = Fixture::new().await;
    *f.mode.lock().unwrap() = "delay-done".into();
    let response = f.send(true, messages()).await;
    let mut stream = response.bytes_stream();
    let first = stream.next().await.unwrap().unwrap();
    assert!(!String::from_utf8_lossy(&first).contains("message_stop"));
    assert!(
        !f.log
            .lock()
            .unwrap()
            .iter()
            .any(|(p, _, _)| p.ends_with("commit"))
    );
    drop(stream);
    f.settled("abort").await;
}
#[tokio::test]
async fn done_release_commits_and_truncated_done_aborts() {
    for mode in [
        "",
        "mixed-delimiters",
        "bytewise",
        "truncated",
        "oversized-stream",
    ] {
        let f = Fixture::new().await;
        *f.mode.lock().unwrap() = mode.into();
        let result = f.send(true, messages()).await.text().await;
        if mode != "truncated" && mode != "oversized-stream" {
            assert!(result.unwrap().contains("message_stop"));
            f.settled("commit").await;
        } else {
            assert!(result.is_err());
            f.settled("abort").await;
        }
    }
}
#[tokio::test]
async fn bad_codec_settlement_ack_quarantines_owner() {
    let f = Fixture::new().await;
    *f.mode.lock().unwrap() = "bad-ack".into();
    let _ = f.send(false, messages()).await.bytes().await;
    f.settled("commit").await;
    let next = f.send(false, messages()).await;
    assert!(!next.status().is_success());
    let log = f.log.lock().unwrap();
    assert_eq!(
        log.iter()
            .filter(|(p, _, _)| p.ends_with("prepare"))
            .count(),
        1
    );
}
