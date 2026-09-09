# Hardened container isolation — security posture

The folder-project container backend runs workflow agents inside an unprivileged
Docker container over controller-seeded, per-run named volumes. It is designed to
protect the operator/controller environment from actively misbehaving workflow
agents within the stated boundary below.

## Trusted boundary

Trusted: the operator/controller process, host, and Docker daemon. Untrusted:
workflow agents, repository code, generated scripts, and artifacts produced by a
run. This does not claim protection against a compromised host, Docker daemon, or
malicious human operator.

## Enforced boundary

- The agent-facing container does **not** mount the live worktree, live `.git`,
  host home, credential files, or the Docker socket.
- The controller seeds a per-run workspace named volume before the run starts.
  Credential paths (`.env*`, `.npmrc`, `.netrc`, `.aws`, `.docker`, `.config/gh`)
  are refused. Live Git directories are never copied. Declared repository inputs
  may use fresh controller-generated shallow Git metadata containing only the
  pinned commit and its tree/blob objects, not parent history or local hooks/config.
- Agent processes run as the non-root `archon` user with `--cap-drop ALL`,
  `--security-opt no-new-privileges`, `--read-only` image filesystem, explicit
  `--memory` and `--pids-limit`, and `--network none`. Optional provider/registry egress is available only through a controller-pinned CONNECT proxy over a per-run Unix-socket volume; Docker bridge/NAT remains unavailable to the agent container.
- Runtime state that must be writable lives on per-run named volumes or small
  tmpfs mounts (`/tmp`, `/run`).
- Host `process.env` does not cross into container exec calls. Providers and
  deterministic subprocesses receive only the Archon-managed env bag, with
  PATH/HOME/PWD-like keys denied so the image controls binary and home lookup.
- Container provider requests reject unpinned MCP configurations and host in-process
  native tools before config interpolation. Claude settings sources default to an
  empty list; nonempty sources require future pinned-bundle support and are refused.
  Codex forwards only explicitly supplied provider credentials, never ambient host keys.
- Unsupported write-back from the hardened volume to the live worktree fails
  closed until a controller-owned publication/write-back action exists.
- Configured backend egress requires explicit HTTP grants in addition to transport
  targets. The TLS-terminating gateway binds CONNECT, SNI and HTTP Host, checks
  methods/paths, and verifies public-address upstream TLS. Legacy CONNECT-only
  policies cannot start or resume a hardened backend environment.

## Strict HTTPS gateway (production admission disabled)

`src/egress/strict-https-proxy.ts` provides a separately tested Node TLS-terminating
gateway. Controller-supplied certificate material and HTTP grants bind CONNECT,
SNI, HTTP Host, method and request path; upstream certificates are verified. Its
bounded HTTP/1.1 subset refuses WebSockets, HTTP/2 and chunked request bodies
rather than falling back to an opaque tunnel. Cancellation, pending DNS admission,
partial uploads, slow readers and truncated responses have real local TLS tests.

The backend generates a per-run ephemeral CA and leaf through fixed system OpenSSL.
The CA private key is never returned. Only the proxy mounts the read-only private
TLS volume; agents receive a read-only public CA and Unix socket volume. Staging
uses only CHOWN beyond dropped capabilities and never mounts live host directories.
The launcher checks bounded, nonlinked, appropriately owned private files against
the exact frozen policy. Serving ends at the earlier CA/leaf expiry. Resume preserves
those credentials and rejects missing volumes, expired material or proxy binding drift;
cleanup remains possible after expiry. No certificate regeneration occurs on resume.

Real container fixtures cover approved HTTPS, forged Host and method denial, private
key absence, suspend/resume and expiry shutdown. They do not certify installation,
provider authentication or authoritative usage accounting. Repository configuration
cannot enable egress by itself. The existing private
`controller-policy/planning-approval.json` may include `egress: { image, policy }`:
`image` must equal the resolved immutable runner ID and `policy` must specify strict
transport targets and HTTP grants. The policy's workflow digest must match the run.
This is operator-owned configuration, not an agent artifact or approval substitute.

Every new controller session is now v2 and HMAC-bound, including sessions with no
egress or action grants. Signing keys and bindings must occupy fixed direct paths
under `controller-runs/<runId>-<UUID>`; imported snapshots cannot supply them. On
resume, copied policy and session authentication precede backend effects. Captured
image, egress and owner bindings are checked against controller authority and actual
Docker resource labels. Trusted private run markers prevent host-routing downgrades.
Unsigned/v1 sessions require a fresh guarded run, not reconstructed authority.

Production activation remains disabled pending authoritative budgets, final receipt
services and the complete hardened run contract. These controls do not authorize
publication or backfill writes.

## Browser observation profile (not release authority)

The optional browser observation service runs the application and verifier in separate
containers sharing a network namespace from a network-none pod. The application has no
mount of verifier definitions or evidence. Only the verifier uses the custom seccomp
profile; workflow agents, applications, seed helpers and artifact readers retain Docker's
default filter. The controller-only volume initializer may use UID 0 and CHOWN on a fresh
named volume; it executes no repository code.

The source profile is pinned to [Playwright v1.60.0 commit
87bb9dd](https://github.com/microsoft/playwright/blob/87bb9ddbd78f329df18c2b24847bc9409240cd07/utils/docker/seccomp_profile.json).
Its SHA256 is `cc3e61cabda6bbc1e53e54d27ba4d55a9d3be829b6dd1a596f4a7b31b1cc7849`.
The reviewed effective profile removes io_uring syscalls and allows the chroot syscall
for Chromium's nested user-namespace sandbox, without adding container capabilities.
Its SHA256 is `153cb94e0bb74823af2e2e4e8548fc5a0895639e4bf16fa98ecac258b5641019`.
A seccomp option alone is not proof: tests inspect kernel filtering, no-new-privileges,
UID/GID and capability state and run harmless denial controls. There is no no-sandbox,
unconfined-seccomp, host-IPC or SYS_ADMIN fallback.

Imported observations explicitly have `authority: none`. Image labels are not build
attestations. Publication requires separate immutable build/oracle/approval binding and
a trusted receipt handler. Request cancellation, total observation deadlines and
owned-resource cleanup have isolated container tests. Binding this service to the
workflow's durable cumulative budget and final receipt authority remains required
before production admission.

## Out of scope

- Docker daemon or kernel escape vulnerabilities.
- Secrets explicitly handed to an agent process as part of the approved managed
  credential envelope.
- Malicious controller/operator behavior.
- Build-time installer compromise. Tool versions and the Debian base digest are
  pinned, but vendor installer scripts are still fetched over TLS at image build
  time and are not independently checksum-verified.

## Review gate

Any change that adds a host bind mount, broadens capabilities, runs agent work as
root, mounts the Docker socket, forwards host env, or copies hidden credential/VCS
state into the agent workspace must be treated as a security-boundary change and
reviewed against this document.

## Unclosed rollout requirements

Planning snapshots and provider-reported token counters are not release certificates.
Independent immutable-commit test/browser execution and publication/backfill handlers
remain gated. Token enforcement against unmanaged in-container API calls or forged
provider transport usage has not been established; the CONNECT transport alone is
not an authoritative billing meter. Do not enable production admission on the basis
of helper/schema or fake-provider fixture tests alone.
