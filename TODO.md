# TODO (short term)

## Goal
Keep tagging stable with minimal, low-risk fixes until v2 work is prioritized.

## Current temporary changes
- [x] Make datetime transformations accept both datetime/date objects and supported datetime strings.
- [ ] Add one DAG-like regression test for string-backed `datetime` in tagging rules.
- [ ] Audit dynamic tag names for case/spacing consistency to avoid dependency warnings.

## Guardrails
- Avoid changing core tag/rule graph behavior unless there is a production bug.
- Keep `tags` storage as comma-separated string for now.
- Keep transaction-level `currency` behavior unchanged for now.

## Notes
- Supported datetime string formats currently follow `calendar_utils` parsing (`YYYY-MM-DD`, `YYYY-MM-DD HH:MM:SS`).

