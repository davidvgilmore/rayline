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
cargo run -p rayline-local-router --locked --example arc-router -- /private/config.json 20811
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

The caller owns causal history, compaction, held-model selection, and attribution
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
3. Drive those contexts through `arc-router`; capture worker requests and a local
   recording provider's model/thinking fields. Establish the same decisions and
   dispatch semantics without paid provider calls.
4. Exercise unavailable worker, package mismatch and ineligible decision errors;
   establish no provider dispatch and no fallback inference.

A golden-only match does not establish VSR-Lab parity unless the reference
engine, package, inputs and non-token state are the same. Automatic episode
tracking, managed runtime installation and general client launch integration are
subsequent work, not properties established by this experimental adapter.
