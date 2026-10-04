# Experimental local ARC adapter

This branch integrates an operator-managed ARC decision worker with Rayline's
existing Messages forwarding path. It does not ship a model runtime, download
weights, or establish numerical parity. The worker must execute the encoder and
heads locally to qualify as on-device inference.

## Integration choice

A C ABI would avoid IPC, but would introduce native runtime linking and unsafe
boundary ownership into the router. A loopback worker follows Rayline's existing
`llama-server` process boundary and lets inference evolve independently. The
adapter therefore lives in a focused `rayline-local-router::arc` module. Automatic
worker lifecycle belongs in `rayline-daemon` once the shared runtime is available.
No ARC head mathematics is implemented in the routing crate.

## Configure and run

Add an `arc` object to a normal router configuration. All values below are
synthetic; supply the exact private package identity and action bindings through
an external local configuration, never by committing them here.

```json
{
  "endpoints": [{
    "id": "local-provider",
    "protocol": "anthropic_messages",
    "base_url": "http://127.0.0.1:9002"
  }],
  "arc": {
    "base_url": "http://127.0.0.1:9001",
    "package_alias": "synthetic",
    "package_sha256": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    "timeout_ms": 30000,
    "bindings": {
      "synthetic-action": {
        "target": {"endpoint": "local-provider", "model": "synthetic-model"},
        "request_overrides": {"thinking": {"type": "disabled"}}
      }
    }
  }
}
```

```sh
cargo build --locked -p rayline-cli -p rayline-daemon
./target/debug/rld serve --no-local-model --router-config-path /private/config.json
```

This uses the ordinary foreground daemon and its `/v1/messages` endpoint on
port 20811. Stop it with Ctrl-C. `--no-local-model` skips the bundled generation
model; the separately managed ARC worker and configured provider endpoints must
already be running. Successful daemon startup does not establish worker readiness
or model parity; a worker failure is returned on the ARC request.

For decision-only replay during integration development:

```sh
cargo run -p rayline-local-router --locked --example arc-replay -- /private/config.json /private/requests.jsonl > /private/decisions.jsonl
```

`arc-replay` sends exact existing
`rayline.arc.policy-decision-request.v1` envelopes to
`POST /v1/rayline/arc/policy/decide` and writes the full responses to stdout.
Keep inputs and output outside the repository; output may contain private
package and model metadata. Requests are processed sequentially.

For actual routing, POST normal Anthropic Messages requests to Rayline's
`/v1/messages`, set `model` to `rayline-arc`, and include a `rayline_arc` object
containing the existing decide envelope **without** `request`. Include explicit
`schema_version`, package identity, episode hash, context epoch,
`request_format: anthropic_messages`, attribution and selection. The adapter
injects the incoming request after removing `rayline_arc`; neither context nor
inbound credentials are sent to the provider. Worker credentials are not needed:
the worker origin must be a literal loopback HTTP address, redirects and proxies
are disabled, and a timeout is mandatory.

In explicit replay mode, the caller owns causal history, compaction, held-model selection, and attribution
of completed provider responses. Use a parity replay driver that supplies the
same state as the reference. The adapter does not infer missing state, maintain a
ledger, or populate empty attribution for existing histories. Missing context and
worker failures fail the request; static routing does not substitute for ARC.
Responses API requests and local redirect bindings are currently rejected.

The response must match the pinned package and schema, choose a configured and
offered action, carry a trained arm, and list that action as selected and
available. Bindings determine the forwarded model and thinking semantics;
caller-supplied thinking/output configuration is replaced. Only `thinking` and
`output_config` overrides are supported. Other action semantics require an
explicit adapter extension before that action is enabled.

## Acceptance evidence

Default tests use synthetic loopback workers and providers. They establish
request-to-decision-to-dispatch composition and failure checks, not model parity.
A release acceptance run must independently establish all of these:

1. Load the exact HF commit and safetensors locally; verify package identity and
   record backend, precision and conversion provenance.
2. Replay the same reference decide envelopes, including sequential held turns,
   attributed history, masks and compaction. Compare exact action and arm IDs,
   and numerical scores under tolerances fixed before evaluating the candidate.
3. Drive those contexts through `rld serve`; capture worker requests and a local
   recording provider's model/thinking fields. Establish the same decisions and
   dispatch semantics without paid provider calls.
4. Exercise unavailable worker, package mismatch and ineligible decision errors;
   establish no provider dispatch and no fallback inference.

A golden-only match does not establish VSR-Lab parity unless the reference
engine, package, inputs and non-token state are the same. Automatic episode
tracking is available through the opt-in session service described below. Managed
runtime installation and general client launch integration remain subsequent work.

Rayline's product surfaces are its CLI and daemon, including launchers for
external desktop clients. `rayline codex app` is a Codex desktop launcher; it
does not establish ARC support for Codex's Responses API. The explicit replay path requires caller-authored context. The opt-in Messages
session path below accepts native Claude session identity without that context.
Responses support and managed worker lifecycle still require further integration.

## Opt-in interactive Messages sessions

An operator-managed session service can now replace caller-authored replay
context. Add the following field inside `arc` while retaining the pinned package
and complete action bindings:

```json
"session": {
  "base_url": "http://127.0.0.1:9013",
  "timeout_ms": 30000,
  "max_response_bytes": 16777216
}
```

Start the normal daemon with that router configuration. A client sends ordinary
`/v1/messages` requests with `model: rayline-arc`; omit `rayline_arc`. Native
`x-client-session-id`, `x-claude-code-session-id`, or the `session_id` inside
Claude's JSON `metadata.user_id` identifies the conversation. Agent and parent
agent headers keep child sessions separate. An unidentified conversation is
refused rather than joined to an anonymous shared history. Rayline creates its
own process owner and a fresh operation ID for every HTTP attempt. A daemon
restart creates a fresh owner; old history starts with unknown attribution.
Existing requests containing `rayline_arc` continue to use explicit replay.

The focused `arc_session` module implements the loopback transport contract:

* `POST /experimental/arc/session/prepare` receives owner, operation, native
  identity, `request_format: anthropic_messages`, full native request and the
  configured eligible action IDs. The service owns policy cadence, history
  epochs, attribution and private on-change steering. Its receipt supplies the
  pinned numerical decision, selected action, session token and provider request.
* Rayline validates the package, bound action/model, numerical revision and arm
  identity, then sends the prepared native request through the configured
  endpoint's ordinary authentication path. Service bindings must validate the
  arm against the pinned package; the host verifies its shape and identity.
* Every action for one endpoint/model must configure the same native
  `thinking`/`output_config` values. Rayline requires those exact values in the
  prepared request and bypasses model-name thinking adaptation. Stage 2 steering
  remains private message text; it never changes these native controls.
* The response bytes and usage remain unchanged. Rayline observes a bounded
  copy, reconstructs native signed thinking and tool blocks, and sends `commit`
  only after a complete successful Messages body or terminal SSE response has
  passed through the response-body consumer. A consumer that closes immediately
  after the terminal SSE frame still commits; a continuation waits for that
  acknowledgement through a per-conversation gate. The exact native assistant content
  is supplied for attribution. This is transport acceptance, not a claim of
  remote client acknowledgement or that the client will preserve every field.
* Provider errors, malformed/incomplete responses, observation overflow and
  dropped bodies abort. An acknowledged cancellation permits another attempt;
  ambiguous prepare/settlement blocks this owner until the operator resolves
  the service state and restarts. The host never silently retries generation.
  Disconnect also stops the owned provider response stream.

The service must expose idempotent commit/abort acknowledgements, retain held
worker selection on retries, and keep private text outside the client-visible
history. Its response is trusted for ledger placement; numerical inference,
planner semantics and pinned trained-arm membership remain service-owned.
This adapter does not bundle or supervise that service. The private reference
implementation is not included in this repository. A distributable session
service, clean installation, Responses API support, explicit compaction events,
and real on-device encoder parity remain unqualified.

Synthetic tests now compose two ordinary HTTP requests through Rayline, a mock
session service and a mock provider. They verify native session identity,
prepare/dispatch/commit, private text after a tool result, cache intent, fixed
worker thinking, signed/tool content and clean client history. Separate streaming
and cancellation tests cover byte chunking, terminal validation and abort.
These prove the public adapter's transport contract, not the service's planner
or live model behavior. The earlier caller-managed limitations apply to replay;
interactive session mode removes the need to construct those replay envelopes.
