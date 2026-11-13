# Phosphor patch-for-the-patch: telemetry command completion

This is a downstream follow-up to the telemetry lifecycle patch in Dagger
1.0.0-beta.23, base f0a3d728cf7012e0954f772b5dc5c8471648afaa, and its
otel-go base 132b1dd82eb2. It is deliberately labelled in each changed code
path so an upstream merge conflict does not accidentally restore blocking
remote export. It does not replace upstream history.

Preserve these behaviors on rebase:

- Drain real local command output and preserve command exit status.
- Give remote provider shutdown one 250 ms caller-controlled budget, with
  concurrent signals and a deadline that survives an uncooperative SDK.
- Never synchronously re-export remote metrics from engine stream consumers.
  Queue only remote destinations; keep local frontend metrics independent.
- Freeze aggregation, bucket and exemplar slices; retain context values.
- Bound waiting batches and encoded payload including in-flight data; make
  overflow, cancellation and unfinished flush visible.
- Keep the upstream engine protocol/image version; the release suffix identifies
  this client fork, not a new engine protocol.

Acceptance: telemetry package race tests, native Linux CLI configuration/output
tests, and the real CLI against healthy, 503, stalled and refused sinks. Check
exact stdout, real command failure and live shared-backend trace/log readback.
A smaller shutdown deadline alone is insufficient: the synchronous metric
forwarding path independently blocked larger-output commands in the reproducer.

Upstream removal criterion: equivalent tests pass against the proposed upstream
replacement without this follow-up. An HTTP 200 alone is not delivery proof.
