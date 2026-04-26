# mecon v2 roadmap

## North star
Define and enforce a canonical transaction schema at data boundaries while preserving existing rule semantics.

## Canonical schema target
- `datetime`: datetime-like (timezone policy documented)
- `id`: string
- `amount`: float
- `amount_cur`: float
- `currency`: 3-letter uppercase code at transaction level
- `description`: string
- `tags`: canonical representation to be decided (`set[str]` vs csv string + adapters)

## Phased rollout

### Phase 1 - observability and safety
- Add strict schema validation utility with report mode (no hard fail).
- Add counters/logging for coercions and invalid rows in ingestion paths.
- Ensure all high-volume entry points call `Transactions.from_dataframe`.

### Phase 2 - boundary normalization
- Switch ingestion and session boundaries to normalized constructors.
- Add fail-fast mode behind feature flag/config.
- Expand tests for malformed input coercion/fail behavior.

### Phase 3 - tags and currency model cleanup
- Decide and implement tags canonical model.
- Decide behavior for multi-currency aggregated rows.
- Update aggregators, lookup helpers, and comparisons accordingly.

### Phase 4 - deprecations and docs
- Deprecate legacy shapes/APIs with migration notes.
- Publish examples and compatibility matrix for rule JSON.

## Open decisions
1. Tags canonical representation: `set[str]` vs csv string compatibility layer.
2. Multi-currency grouping output model.
3. Timezone policy for datetime normalization.

