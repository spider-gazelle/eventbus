# Suppress signage heartbeat changefeed events — plan only

## Recommendation

Filter telemetry-only sys updates at the EventBus SQL UPDATE trigger. Ignore changes confined to signage_last_seen and playlist_item_id; continue notifying for every other changed column, including a mixed telemetry/configuration update. This deliberately applies to all writers of these telemetry fields, not only update_last_seen_time. The existing method can retain its single UPDATE and database constraints.

Use a dedicated AFTER UPDATE trigger with this WHEN expression:

```sql
(to_jsonb(OLD) - ARRAY['signage_last_seen', 'playlist_item_id'])
IS DISTINCT FROM
(to_jsonb(NEW) - ARRAY['signage_last_seen', 'playlist_item_id'])
```

Keep INSERT and DELETE unconditional in a separate trigger. A combined INSERT/UPDATE/DELETE trigger cannot use this OLD/NEW comparison in WHEN. PostgreSQL evaluates an AFTER-trigger WHEN before queueing the trigger, avoiding EventBus's JSON diff, event-table INSERT, cleanup hook, NOTIFY and downstream work. Row serialization for the predicate and the original sys write still occur. No-op sys updates also become silent.

## Per-model configuration (revised)

The model is the source of configuration; do not maintain a global model/column list or a PlaceOS-specific EventBus policy. Proposed API (not yet implemented):

```crystal
class PlaceOS::Model::ControlSystem < PlaceOS::Model::ModelBase
  # Suppress UPDATE events when only these attributes change.
  changefeed_ignore_updates :signage_last_seen, :playlist_item_id
end
```

- pg-orm exposes the macro and model metadata, validates persisted attribute names at compile time where possible, and resolves database column names. Define and test inheritance/override behavior so sibling models never share mutable configuration.
- Pass the optional per-model policy along Model.changes -> Database.listen_change_feed -> ChangeFeedHandler.add_listener -> EventBus.ensure_cdc_for(table, ignore_update_columns: ...).
- EventBus remains generic and validates actual database columns before building the trigger predicate. Unconfigured tables retain their existing behavior, including no-op updates.
- An omitted policy means preserve the installed policy (or create the default trigger if absent). An explicit empty list means clear filtering through a deliberate policy replacement operation; ordinary default models must not send an empty list implicitly.
- The installed trigger definition and per-table metadata, if needed, are database deployment state, not a second user-maintained configuration list. Bulk ensure must preserve that state. Prefer storing only the metadata needed for robust reconciliation; no policy-table lookup on each UPDATE.
- Repeated identical declarations are idempotent. Conflicting explicit declarations must fail clearly, not silently win by startup order. Provide an explicit expected-current-policy replacement/reset path for intentional policy changes and rollback, under the same advisory lock as trigger DDL.
- Model subscription installs the declared policy before returning its changefeed. Suppression is effective after registration; earlier writes can still produce events. Verify service startup ordering. If suppression must precede every writer, add an explicit per-model registration step using the same declaration, not a duplicated SQL migration list.
- Publish EventBus, then pg-orm, then models dependency updates. Roll out compatible installers across CDC-initializing services before enabling the ControlSystem declaration. Older installers do not honor the new policy semantics and can restore the old combined trigger.

## Findings

- src/placeos-models/control_system.cr:78 writes both telemetry columns directly, without updating updated_at.
- spec/playlist_item_spec.cr already tests persisted timestamp and current item, but not CDC suppression.
- EventBus owns SQL trigger installation in its src/eventbus/init.cr. Its current trigger matcher requires all three actions and does not inspect predicates. A models-only trigger replacement would be overwritten on reconciliation.
- Core mappings/control_system_modules.cr:51 refreshes logic modules even if module membership is unchanged.
- The signage API writes this heartbeat on non-preview display requests, including requests whose content is unchanged. Repository searches found no explicit heartbeat-field changefeed consumer, but generic changefeed/search consumers must be audited before implementation.

## Implementation and verification checklist

- [x] Trace models, EventBus installation and core refresh path.
- [x] Compare database filtering, scoped suppression and core filtering.
- [ ] Claim the implementation issue in an isolated ai/ branch/worktree and save the detailed plan in a draft PR before coding.
- [ ] Write a failing integration regression: real CDC enabled, update_last_seen_time persists both fields but must create no sys CDC event. Test changed item, unchanged item and clearing with nil/empty input. Use committed writes and a listener barrier/bounded wait rather than an arbitrary sleep.
- [ ] Add generic per-table ignored-update-column support in the local eventbus project with the declaration/preserve/conflict/reset semantics above. Validate and safely encode names; test bulk installation and concurrent registrations.
- [ ] Extend EventBus trigger installation, matching, removal and upgrade tests for unconditional insert/delete plus filtered update triggers. Apply replacements atomically under existing advisory locks and bounded DDL retries. Verify existing DB upgrades, fresh DB setup and repeated startup without DDL churn.
- [ ] Add the per-model macro and policy propagation in the local pg-orm project; test valid/invalid fields, defaults, inheritance, sibling isolation and subscription registration.
- [ ] Declare the two ignored attributes on ControlSystem in models. Keep update_last_seen_time a single UPDATE; document why it is silent. No models migration containing a separate policy list; rollback uses the explicit EventBus policy reset.
- [ ] Verify timestamp/item persistence, no CDC row and no notification for heartbeat-only updates; ordinary and mixed configuration updates still notify exactly once; inserts/deletes and unrelated tables remain unchanged; no-op behavior is explicit; rollback restores original notifications. Test concurrent heartbeat/configuration writers.
- [ ] Audit generic consumers: suppressing this event also removes heartbeat CDC history and live/search updates. Preserve SQL-backed monitoring. If any consumer requires those events, reassess the scope before implementation.
- [ ] Measure repeated heartbeat event counts and core refresh counts before/after. Check the predicate cost against the existing JSON diff/event write path; do not claim the database UPDATE itself is free.
- [ ] Run formatting/lint and affected specs through one test agent at a time, then GitHub CI for each changed repository. Use fresh targeted test DB/images to avoid stale migration caches.
- [ ] Release EventBus and update consumers' resolved dependencies before enabling the policy. Old EventBus installers would restore the combined trigger; verify every CDC-initializing service and mixed-version restart behavior. Apply models policy only after compatible installers are deployed. Keep rollback documented.

## Alternatives

- Method-scoped suppression: add an EventBus WHEN predicate using a custom transaction-local setting, set it around the UPDATE on the same connection and restore the previous value before other work in an outer transaction. Requires tests for errors, rollback, nesting, connection reuse and concurrent writers. This matches the exact method boundary but adds transaction/settings overhead and can inadvertently silence unrelated writes if scoped incorrectly. Never disable table triggers or use session_replication_role.
- Early return in the SQL trigger function: avoids event creation/notification but still invokes the trigger. Simpler fallback if trigger-policy support proves disproportionate.
- Core-only filter: inspect the changed-field list and skip only updates confined to the two telemetry fields. Preserve create/delete and fall through on missing/unknown change information. Saves driver refreshes but not EventBus overhead; use only if other consumers require heartbeat events.

## Review

Planning only. No implementation or tests run. There is no suitable current milestone in PlaceOS/local (only the old 1.2110.0 milestone); leave this standalone issue without a milestone rather than attach it to an unrelated release. Estimate after the consumer audit and EventBus policy design. Next action is review of this proposal, then implementation if requested.
