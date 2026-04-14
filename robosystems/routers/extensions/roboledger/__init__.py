"""RoboLedger command operations.

Hosts the `/extensions/roboledger/{graph_id}/operations/*` endpoints.
Each operation is a thin route that opens a session, calls into
`operations/roboledger/commands/*`, and returns an `OperationEnvelope`
via `execute_operation`.

**Registered today:**
- `update-entity`

**Deferred to follow-up** (need `operations/roboledger/commands/*`
extraction first — most already have service-class equivalents that
would wrap trivially):

- Fiscal calendar / periods: `initialize`, `set-close-target`,
  `close-period`, `reopen-period` (wrap `FiscalCalendarService` +
  `PeriodCloseService`)
- Schedules: `create-schedule`, `truncate-schedule`,
  `create-closing-entry`, `create-manual-closing-entry` (wrap
  `ScheduleService`); `auto-map-elements` (dispatch to worker,
  returns `status: "pending"`)
- Taxonomies: `create-taxonomy`, `create-structure`,
  `create-mapping-association`, `delete-mapping-association` (inline
  in old router today — extract to commands layer)
- Reports: `create-report`, `regenerate-report`, `delete-report`,
  `share-report` (entire reports router deferred per step 2 note)
- Publish lists: all 5 operations (entire publish_lists router
  deferred per step 2 note)

The cutover PR requires complete coverage — these must be added
before the old `/v1/ledger/*` routes can be deleted.
"""
