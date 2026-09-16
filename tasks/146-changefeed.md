# Per-table update filtering

Parent issue: https://github.com/PlaceOS/local/issues/146
PR: https://github.com/spider-gazelle/eventbus/pull/6
Agent/task: codex-146-eventbus-20260916-root
Branch: ai/146-model-changefeed
Start: 2026-09-16

## Contract

- `ensure_cdc_for(table, ignore_update_columns: columns)` installs a per-table update filter; ORM model metadata will supply this in the next release stage.
- Omitted configuration preserves the installed policy. Empty configuration on an unconfigured table retains default behavior. Identical declarations are idempotent; conflicting nonempty policies raise.
- `replace_cdc_update_policy` requires the expected current columns before changing/resetting shared state.
- The filtered UPDATE trigger compares old/new JSONB excluding configured columns; INSERT/DELETE remain unconditional. Mixed updates notify with the full original payload. Default tables retain no-op notifications.
- Column names must exist and cannot exclude `id`. Names are quoted safely. No global configuration list and no metadata query in the row-update path.
- Redundant versioned comments on managed triggers preserve policy across reconciling subscribers, bulk ensure and partial trigger damage. Forced uninstall removes both triggers and metadata.
- All trigger-installing services must upgrade before enabling the policy; old installers can cause duplicate events. A reset may require an exclusive table lock, bounded by existing retries/timeouts.

## Checklist

- [x] Read local instructions and trace registration/SQL behavior.
- [x] Claim parent issue and create isolated worktree/draft PR with plan before implementation.
- [x] Write regression specs; verify original source rejects the new API.
- [x] Implement SQL filtering, validation, policy conflicts/replacement, reconciliation and uninstall.
- [x] Independent review: restore missing redundant metadata and document reset locking/legacy rollout.
- [x] Fix existing tooling blockers: Ameba 1.7 compatibility/build, new lint findings, absent optional Docker mounts, focused-spec Redis require.
- [x] Pass full Docker specs: 38 examples, 0 failures/errors; formatting and lint clean.
- [ ] Pass GitHub CI on final commit; finalize review and commit/merge for release.
- [ ] Handoff to user for EventBus release; keep pg-orm/models unchanged until prerequisite releases.

## Verification plan

Exercise ignored timestamp/item changes including NULL transitions, no-op behavior, configuration and mixed updates, INSERT/DELETE, real listener delivery, invalid columns, policy replacement conflicts, concurrent registration/writes, atomic rollback, quoted names, normal/forced disable, bulk and repeated startup, legacy trigger drift and recovery of missing triggers/comments. Existing locking, cleanup and event dispatch specs must remain green. Only one suite agent runs at a time.

## Review

Implementation review found a metadata-redundancy gap: an otherwise correct trigger pair could retain only one policy comment. Reconciliation now restores missing metadata before succeeding. Final local verification: `./test` passed 38 examples with 0 failures/errors in 9.18 seconds. `./bin/ameba`, `crystal tool format --check` and `git diff --check` pass. Final review also caught an unnecessary DROP of an absent helper trigger on default installation; this is guarded and covered under an active reader lock. GitHub CI is required before release handoff. The user will publish EventBus before work begins in pg-orm, then publish pg-orm before models is updated.
