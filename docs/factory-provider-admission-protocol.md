# Factory provider admission protocol v1

Normative source: `packages/providers/src/factory-mode.ts` (`configSchema` and `leaseSchema`) and `factory-admission.ts`. Both producer and consumer must test the shared golden wire fixture `packages/providers/test/fixtures/factory-provider-broker.v1.json`. Its capability is synthetic test data.

## Trusted startup

The factory parent passes `--factory-provider-broker-fd 3`, writes one UTF-8 JSON config to the inherited descriptor, and closes its write end. The engine consumes at most32KiB and closes the descriptor before provider registration/SDK construction. The capability never appears in provider argv, environment, workflow artifacts or a readable configuration file. Invalid, missing, expired or conflicting configuration aborts startup. Project environment files cannot disable an argv-selected mode.

Config version is `factory.provider-broker.config.v1`. Required fields: transport, endpoint, capability, expiresAt, managedRun, providerPolicy. Unknown fields reject. Transport is `http+unix` with an absolute socket-path endpoint, or `http+loopback` with an HTTP loopback endpoint. Unix transport needs separate host qualification. The capability is narrowly bound to this launch, never an operator cookie or a machine bearer.

managedRun contains machineId, hostEpoch, factoryJobId, logicalChainId, attemptId, readySnapshotId, readyDigest, runtimeBundleId, runtimeBindingDigest, projectId and worktreePath. providerPolicy contains providers (provider, exact models, purpose) and allowedWriteRoots, allowedReadRoots, deniedRoots. Supported provider IDs are codex, claude, and grok; unknown managed providers reject. Grok is CLI-backed (`grok` with `--oauth`) and does not admit API-key construction in factory mode. The host independently binds all fields to authorized launch/source/runtime and authoritative Control state.

For native deterministic execution, pass `--factory-provider-offline` without a broker descriptor. Provider construction is blocked and native title generation is deterministic. No test preload is needed. Mode persists as metadata.factory_provider_admission and is required on execution resume and successor adoption. Gate-only response/read commands may omit it. Previously unmarked runs cannot silently become managed continuations. Managed detached execution remains explicitly unqualified; use the foreground owner supervised by the bridge.

## HTTP messages

Use Authorization: Bearer followed by the capability, with JSON bodies. Only POST /acquire and POST /settle exist. Successful replies must be JSON with status200 or201 and bodies bounded to32KiB.

Acquire version is `archon.provider-admission.v1`. Required fields: invocationId, provider, purpose, factoryBinding, context, cwd, model, promptDigest, requestDigest. resumeSessionId is optional. factoryBinding is managedRun without worktreePath. context contains runId and nodeId; iteration and reask are optional integers, with reask counting from zero. Ordinary workflow nodes use purpose implementation; the host maps the frozen node identity to permitted roles. Managed titles never call a provider.

promptDigest is bare lowercase SHA-256 of the prompt UTF-8 bytes; prompt text is never sent. requestDigest is bare lowercase SHA-256 over canonical JSON of every acquire field except requestDigest: sort object keys recursively by code-unit order, preserve array order, omit undefined object values, emit no whitespace. The golden fixture contains an independently computed expected digest.

An uncertain acquire transport is retried once with the exact same body/invocationId. The broker must durably replay that reservation. Changed payload under the same identity rejects. A lost acknowledgement or local broker restart must not authorize duplicate work.

Lease replies require version `archon.provider-admission.v1`, invocationId, requestDigest, leaseId and leaseExpiresAt. Identity/digest/expiry must match before construction. One lease owns the entire sendQuery stream including SDK subprocess retries; this is not per-model-turn billing. Reasks and workflow loop turns acquire distinct invocations. The same provider account must remain exclusive across broker instances through authoritative Control/PostgreSQL state.

Settle requires version, leaseId, invocationId, requestDigest, outcome (released or quarantined), and settledAt; reason is optional. Acknowledgement is an object with the matching outcome. Release requires terminal result, confirmed native transport closure, no pending background tasks and no abort. Exceptions, early consumer return, incomplete tasks, missing closure proof or ambiguous settlement retain quarantine. The engine aborts at lease expiry. TTL/capability expiry alone never makes an uncertain account reusable. This version has no renewal operation.

## Managed file scope and qualification

Broad standalone SDK defaults change only for managed calls. Codex uses a named native permissions profile with minimal runtime reads, explicit readable/writable roots and denied roots; no broad sandbox flag may replace that profile. Claude disables bypass mode, disallows unsandboxed commands, enables sandbox fail-if-unavailable, applies file-tool deny rules and excludes mergeable user/project settings. Path aliases and writable/manual-root overlap reject.

These are narrow native file-access controls, not a complete OS isolation framework. Tool/extension code and provider background behavior require separate qualification. Internal retries retain their stream lease only after previous native transport closure is confirmed. Unsupported configuration/platforms remain unqualified.

Primary semantics: pinned SDK declarations/native binaries, the official [Codex permission compiler](https://github.com/openai/codex/blob/main/codex-rs/core/src/config/permissions.rs), [Claude sandbox documentation](https://code.claude.com/docs/en/sandboxing) and [Claude file-tool rules](https://code.claude.com/docs/en/permissions). Native offline/counted fixtures do not qualify actual inference. This document and golden fixture do not authorize any model execution.

## Qualified binding and bounded successors

The persisted broker marker contains the complete factoryBinding, exact worktreePath, providerPolicyDigest and policyTemplateDigest. Resume requires exact equality before deterministic/bash execution. The actual execution cwd must also equal the configured canonical worktree. PostgreSQL object-key ordering does not affect comparison.

An optional authenticated config successor field contains parentRunId, parentAttemptId, parentBindingDigest and commandId. The bridge may issue it under the already frozen bounded-recovery policy; another human approval is not inherently required. parentBindingDigest is canonical SHA-256 of the parent's complete persisted marker. The selected native --adopt/--supersedes parent must match this authority.

Only an explicitly authorized child creation may change attemptId. All machine/epoch/project/Ready/runtime binding fields remain fixed. Worktree path instantiation may differ only under that parent authority, and the qualified policy must remain identical after replacing worktree-path prefixes in the three filesystem-root arrays with the literal $WORKTREE. Provider/model/purpose entries are never normalized. Exact resume still compares actual worktreePath and providerPolicyDigest. Parent authority is persisted separately in metadata.factory_provider_successor, while normal internal workflow children retain their parent_run_id and current attempt binding.

Keyed launches accept an authorized factory successor whose full parent ID is frozen in the config; parent identity and mode enter the launch digest. Standalone keyed-adoption restrictions remain unchanged. Missing successor authority, changed qualified binding or mismatched lineage fails closed. The shared golden fixture includes parentMarker and successorConfig examples.

## Native temporary directories and test transport

Codex0.150.1 on macOS keeps /tmp (canonical /private/tmp) writable even when the named profile denies a subtree. This boundary cannot protect a manual/protected root there. Such profiles, including aliases, are rejected before managed workflow bash/provider effects; no successful filesystem qualification is implied for that scope. Managed SDK tool caches use /tmp explicitly, so project environment cannot redirect temporary writes into another protected path. Other supported roots retain the real native write-denial test.

A usable Git worktree also needs read-only access to its shared Git metadata directory. Grant that precise .git path through allowedReadRoots, not the whole manual checkout. Native qualification runs git status/diff in a linked worktree and proves the original manual working file remains unwritable. Claude's sandbox allowRead supplies the corresponding specific metadata exception.

The production Node24 bridge transfers config through an inherited pipe. Bun1.3.14's extra-pipe emulation can fail with ENOENT under parallel test load. Engine qualification harnesses therefore create/open a0600 fixture, unlink it BEFORE child spawn, inherit only its anonymous FD as descriptor3, and close the parent FD. Tests assert no directory link remains and the engine closes the descriptor. This does not replace the separate joined production Node pipe/Control/PostgreSQL proof and does not retry or mask spawn failures.
