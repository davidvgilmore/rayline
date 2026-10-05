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
  The selected level and the effective instruction are distinct. The service
  applies steering only at positions admitted by its pinned planner. For example,
  if a Claude request ends with a system date message after the user prompt,
  the planner may skip a selected `up` instruction because that tail is not
  steerable. A later tool-result tail can admit a private user instruction after
  the tool results. Inspect the receipt's effective level, emission and placement;
  do not infer that every selected level was appended or replayed.
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
implementation is not included in this repository. Qualify its exact supplied
artifact and settings using the setup below. Responses API
support, explicit compaction events and real on-device encoder parity remain
separate acceptance requirements.

Synthetic tests now compose two ordinary HTTP requests through Rayline, a mock
session service and a mock provider. They verify native session identity,
prepare/dispatch/commit, private text after a tool result, cache intent, fixed
worker thinking, signed/tool content and clean client history. Separate streaming
and cancellation tests cover byte chunking, terminal validation and abort.
These prove the public adapter's transport contract, not the service's planner
or live model behavior. The earlier caller-managed limitations apply to replay;
interactive session mode removes the need to construct those replay envelopes.

### Messages sessions with prepared Chat providers

An optional `arc.session.codec_sha256` enables a receipt-bound pinned response
codec. Set it to the SHA256 of the operator-managed codec implementation. This
mode still accepts ordinary native `/v1/messages` conversations. It sends their
original source format to the ARC session service, which prepares the final
Chat request and owns private steering and history conversion.

Configure a named endpoint with `protocol: "openai_chat"` and its normal
`base_url` ending in `/v1`. Bind the action to the exact provider model ID. Chat
bindings accept only fixed `reasoning_effort`, `reasoning`, or
`chat_template_kwargs` overrides; use an empty object for an explicit provider
default. All actions for the same worker must have identical controls. Native
Messages bindings keep `thinking` and `output_config`. Stage 2 does not change
these fixed native controls. The complete configured action basket is submitted;
an unsupported selected action is refused rather than silently replaced.

The prepared receipt must retain `source_request_format: "anthropic_messages"`,
use `request_format: "openai_chat"`, and include an exact `response_codec` object:

```json
{
  "schema_version": "rayline.arc.response-codec.v1",
  "source": "openai_chat",
  "target": "anthropic_messages",
  "implementation_sha256": "<configured SHA256>"
}
```

The local session service adds `/experimental/arc/session/codec`. Calls bind the
receipt owner, token, implementation pin and operation. Buffered `response`
converts the provider body. Streaming uses `stream_start`, sequential
`stream_push` calls carrying complete SSE frames, then `stream_finish`. Every
reply must repeat the implementation pin. Rayline preserves the prepared body
and uses the endpoint's ordinary configured authentication; it sends neither
provider credentials nor inbound client authorization to the codec service.
The ordinary Chat translator is bypassed on this prepared path.

Rayline holds a translated native terminal until it observes a provider finish
reason and a complete `[DONE]` frame, followed by successful codec finalization.
Its existing session observer commits exact accepted native output and retains
the queue through the settlement acknowledgement. A dropped response,
truncated stream, codec refusal, or pin mismatch aborts the attempt. Failed or
ambiguous settlement quarantines the process-owned session service identity;
there is no automatic retry or reset. An HTTP-body consumer accepting the native
terminal is the delivery boundary, not an acknowledgement from a remote client.
Missing usage stays absent; this path does not synthesize provider usage.

A configured loopback Chat endpoint can point at a local model server. This does
not enable the special `local` redirect, download/start a model, or qualify the
bundled llama-server lifecycle. `--no-local-model` remains the way to run the
router with only configured endpoints. Native Messages sessions, explicit replay,
and ordinary non-ARC routes keep their existing behavior; explicit replay cannot
dispatch to a prepared Chat endpoint. Responses ingress and durable codec-stream
restart are not supported by this mode. The private service and codec are
operator-managed dependencies and are not included in the public repository.

### Use an installed session runtime

The ARC session service is an operator-managed dependency. If your deployment
supplies its bundle, install the pinned artifact into a new private directory.
Keep the package manifest, provider settings and complete action catalog outside
this repository. The bundle contains no numerical encoder, heads, model weights
or provider credentials; its separate policy endpoint must already be running.

Use unused ports and isolated runtime/server configuration when testing beside
existing agent sessions. These steps require no global client settings changes.

Set `ARC_RUNTIME` to the installation directory and the other variables below
to your private settings file, package manifest and package alias. Obtain the
settings-bound codec pin from the installed launcher:

```sh
"$ARC_RUNTIME/arc-session" --settings "$ARC_SETTINGS" --package "$ARC_PACKAGE" \
  --package-alias "$ARC_PACKAGE_ALIAS" --describe-config
```

Set `arc.session.codec_sha256` to that result. It identifies the codec plus
provider/backend profiles, not only a binary. Start the same launcher and settings
with `--policy-endpoint http://127.0.0.1:9012/v1/rayline/arc/policy/decide --port 9013`.
Set `arc.session.base_url` to the origin `http://127.0.0.1:9013`; Rayline adds the
session operation paths. Then start the normal `rld serve --no-local-model
--router-config-path /private/router.json`. This does not download or start the
numerical worker or generation endpoints. Use a fresh daemon/config directory
when testing alongside existing agent sessions.

Keep the complete action basket and exact endpoint/model bindings. Fixed native
controls come from the selected release, not an effort label. A llama usage
profile must bind the actual backend revision and executable identity, including
its response model alias. Generic Chat providers can require final accounting
before emitting a translated response, and incomplete cache partitions can be
refused. Unknown usage stays unknown; one profile is not a promise of universal
provider streaming or cache support.

Installation and synthetic transport checks do not establish numeric ARC parity,
real-provider cache behavior or durable sessions. The service currently keeps
state in memory. Resolve uncertain settlement before retrying; a restart or new
session identity is not recovery. Responses ingress, managed numerical-worker
lifecycle and launch readiness remain separate work.

### Launch Claude Code with a fresh ARC profile

For ordinary Claude Code interaction, use the explicit direct connection mode:

```sh
rayline claude --config /private/arc-router.json --via direct \
  --fresh-profile /private/new-arc-session
```

The directory must be absolute and must not exist. This mode requires an ARC
session configuration with named endpoint bindings. It selects `rayline-arc` for
all requests and starts the installed `rld` with `--no-local-model`; it does not
onboard or download a generator. Start the installed session runtime, numerical
worker and generation endpoints separately using their qualified settings.
A ready daemon does not establish their readiness or numerical parity.

The launcher supervises both the daemon and Claude Code. Its foreground daemon
uses private data, configuration and logs under the new directory and dynamically
allocated loopback ports. Claude receives a fresh configuration, empty settings
and MCP configuration, no inherited customizations, and a process-local server
URL. Provider credentials referenced by endpoint `api_key_env` remain available
only to the daemon; Claude receives a nonsecret local placeholder. This path
uses no interception proxy, CA registration, OAuth login, browser launch, or
shared daemon registry. The fresh client disables the background agent view;
its lifetime belongs to this foreground launch. It keeps the caller's working
directory and HOME.

To set Claude's output-token limit explicitly, set
`CLAUDE_CODE_MAX_OUTPUT_TOKENS` before launching direct mode, for example
`CLAUDE_CODE_MAX_OUTPUT_TOKENS=512 rayline claude ...`. The launcher validates a
positive decimal `u64` integer before creating the profile or starting processes
and forwards it only to Claude. Unset leaves Claude's default unchanged; invalid,
zero or overflowing values fail clearly. Model/provider limits still apply.

For a custom route such as `rayline-arc`, you can also explicitly set
`CLAUDE_CODE_MAX_CONTEXT_TOKENS` to the deployment's client context budget.
Direct mode validates the same positive decimal integer format and forwards it
only to Claude; omission keeps Claude's default. This informs the client's
context handling without mapping the route to another model or changing native
thinking controls. An unknown-model warning's assumed window is not an ARC
capacity claim.

The current native development encoder has a separate hard limit of 16,384
projection tokens and refuses oversized input without truncation. Claude and
ARC use different tokenizers and message framing, so a client budget of 16,384
does not guarantee that the ARC projection fits. Leave deployment-appropriate
margin; automatic compaction at that boundary has not been qualified. The
encoder's declared package capacity and a cloud model's context window do not
override this operational limit.

Exit Claude or interrupt the launcher to stop its owned daemon and client
processes. The private directory and logs remain for inspection. Each launch
requires a new directory; resume/continue and client overrides that would select
another model or shared settings are refused. This is a fresh conversation,
not recovery of an uncertain ARC settlement. Inspect and resolve the service's
state before retrying an ambiguous outcome.

`--isolated` retains its existing shared-history overlay behavior and cannot be
combined with `--fresh-profile`. `--via env` remains cloud-only. ARC client
launches through other modes are refused with guidance rather than triggering
generator onboarding, hosted routing, or a shared-profile proxy. The Codex
launcher and Codex desktop use Responses and remain outside this Messages-only
integration. Source and synthetic tests do not establish real-client or numerical
acceptance; qualify the exact installed launcher/runtime before relying on it.
