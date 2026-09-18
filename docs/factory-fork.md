# Factory overlay on vanilla Archon

`Pibomeister/archon-engine` is a fork of `coleam00/Archon`. Forking is allowed. A fat overlay that rewrites `workflow.ts` / `dag-executor.ts` / every `scripts.test` is not — that is what made `git rebase` miserable.

## Branches

This fork keeps **three** branches. Older factory tips are **tags** (`archive/*`), not branches.

| Ref | Rule |
|---|---|
| `dev` | Always identical to `coleam00/Archon` `dev`. Never merge factory into it. |
| `feat/community-grok-provider` | Vanilla community Grok only. PR to coleam00 (#3341). |
| `factory/dev` | The factory overlay: vanilla `dev` + community Grok (until coleam00 merges it) + factory hooks. Rebase onto `origin/dev` after every upstream catch-up. |

Archive tags (commits only, not working branches): `archive/factory-grok-implementer`, `archive/factory-pin-2026-09-17`, `archive/factory-series-83e906e0`, `archive/fix-codex-factory-permissions`.

## Hooks, not forks of the executor

Factory-owned files (`packages/providers/src/factory-*.ts`, `packages/providers/src/factory/`, `packages/core/src/db/workflow-factory.ts`, `packages/workflows/src/factory-human-input.ts`) are free.

Vanilla files get call sites:

- `getAgentProvider()` wraps with `createAdmittedProvider` when `isFactoryManaged()`
- `SendQueryOptions` factory fields via `factory-types.ts` module augmentation
- `cli.ts` imports `factory-mode` (reads `--factory-provider-broker-fd` / `--factory-provider-offline` at startup)
- `dag-executor.ts` stamps `factoryInvocation`
- `executor.ts` stamps `factory_provider_admission` metadata and asserts factory resume/successor
- `community/grok/provider.ts` uses factory argv/home only when `options.factoryScope` is set

Do **not** replace vanilla `repo-tests.ts` / `package-tests.ts` with `run-isolated-tests.ts`.

## Rebase

```bash
git fetch upstream
git checkout factory/dev
git rebase upstream/dev
# conflicts should only appear in hook files
git push --force-with-lease origin factory/dev
git push origin upstream/dev:dev
```

If a rebase fights a non-hook file, extract another hook. Do not resolve a 400-line hunk in `workflow.ts`.

## Grok

Unmanaged: community `provider: grok` (`builtIn: false`, host `~/.grok`, `--oauth`).
Managed: same provider, `createAdmittedProvider` injects `factoryScope`; factory argv/home/sandbox apply. Not a second builtin id.
