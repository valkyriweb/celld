# Retrying an idle peer tunnel

An RPC or DO fetch handler may mutate storage. A buffered request body makes
another send possible; it does not make another execution safe. If the owner
finishes a mutation and its response disappears, automatically replaying the
buffered request can run the handler twice.

`main/peer_tunnel.rs::send_inner` therefore retries a pooled buffered request
only when Hyper returns its original request through `TrySendError::take_message`.
The locked Hyper 1.11.0 HTTP/1 implementation returns that message only before
serialization starts. The retry moves the original request, including its body
and headers, to one fresh tunnel rather than rebuilding it from saved bytes.
If no request is recovered, the underlying transport error remains ambiguous
and the caller stops. A fresh send failure is terminal for this transport attempt;
streamed requests retain their existing no-retry policy.

A failed call can still have executed **once**. This does not provide exactly-once
semantics or make arbitrary application/client retries safe. Typed pre-application
stale-route rejection and never-connected handling remain the caller's separate
policies; generic tunnel failure is not relabeled as either one.

## Regression seams

The public `peer_tunnel::retry_tests` module is included under ordinary `cfg(test)`
in the real celld binary. It exercises the production transport with a real HTTP
peer, not an extracted retry algorithm. Its peer is a transport fixture, not a
Durable Object or S3 store: wire counts do not prove application storage or caller
orchestration. The three tests each exercise both buffered RPC and DO fetch: healthy reuse,
fresh loss and pooled loss after the peer consumes and records a mutation. They
assert exact connection/request counts and the original Hyper error, without
manipulating the private pool.

A fourth test controls the old HTTP/1 connection through Tokio duplex I/O and a
manually polled real Hyper driver. A healthy response earns pooling through the
production body wrapper; no private pool is modified. After the old peer closes,
a counting-waker assertion witnesses checkout and queueing before the driver
observes EOF. The production fallback must preserve the original request's
extension, construct only once, then deliver one mutation over fresh real TCP.
This does not qualify old-tunnel establishment or TCP idle-close timing. A
synchronous TCP close/ack would pause the client's reactor and leave EOF readiness
racy; a sleep would only disguise that race.

All four tests remain **uncompiled and unexecuted** in this preparation. Streamed
requests and full caller orchestration still lack new runtime evidence. The
controlled positive test is a source-prepared control, not a passing result.

Private conformance includes remain behind `celld_internal_tests`, which is a
Rust cfg, not a Cargo feature. That cfg does not isolate dependencies.

The targeted command for a separately admitted build/test environment is below.
The invocation needs an externally enforced, approved process deadline; no
arbitrary per-case timeout substitutes for fault reachability:

```sh
cargo test --locked -p celld --bin celld peer_tunnel::retry_tests -- --test-threads=1
```

This still compiles the full celld dependency graph, including V8. It is not a
standalone Hyper-only build. No replacement runtime, dependency manifest or
private include body is used to claim a lighter production build. The shipped
Dockerfile also includes broader tests and clippy; the targeted command does
not claim those gates passed.

At the full application boundary, regression acceptance must retain these
observations together: healthy owner and warm controls, a fault on the exact
reused tunnel, complete owner success before any response byte is forwarded,
one submitted mutation, one wire attempt, one stored mutation and a failed
public call. Missing controls are incomplete evidence, not a product verdict.
The earlier pinned v0.4.0/v0.4.1 run reproduced duplicate mutations in both;
those archived counterexamples remain failures rather than exceptions waived
for a green result.

The lue-kube application fixture is maintained separately from this owning source
module. Buffered DO fetch acceptance, native arm64 behavior, ownership-generation
recovery, mixed versions, persistence and restore need their own evidence. A
transport regression is not a substitute for any of them.

## Prepared-source status

This branch is based on v0.5.0 commit
`12d5b6333fe52717325addcfe1e99e9fd4f77bcd`. The defect is unchanged from v0.4.1:
v0.5.0's only peer_tunnel change is to WebSocket close-frame handling in the
splice path, and `send_inner` is byte-identical between the two releases.
The implementation and test source are
prepared, but **not compiled or runtime-verified** in this work. Formatting and
read-only source review do not establish that they build. The application oracle's
in-memory controls are separate from execution of celld.

Before a test/build run, freeze the final source/test bytes, native toolchain,
complete dependency inputs, resource bounds, evidence and owned cleanup. The
old released-image campaign's small service limits are not a V8 build budget.
A built candidate needs distinct source/binary/image provenance; it must not be
represented as the unchanged upstream release merely because Cargo's package
version remains `0.5.0`. No registry publication, upgrade or live rollout follows
from this source change.
