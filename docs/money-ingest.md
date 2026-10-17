# Corrections and statement attribution

This contract covers how callers retry money corrections and how imported
vendor statements attribute ledger amounts to their lines. The operations are
`spend.correction.ingest` and `spend.statement.import`; their request fields
and refusal codes are in the [module surface](module-surface.md#3-write-operations).

## 1. Caller corrections

1. `spend.correction.ingest` requires `correction_id`, a caller-chosen,
   non-empty string of at most 128 UTF-8 bytes. Keys are unique across all
   corrections, not just within an event.
2. Store the key before sending a correction and reuse it for retries. Choose
   a different key for each separate correction, even if its amount equals an
   earlier one.
3. The other required fields are `event_id`, `currency`, `amount_nanos`,
   `reason` and `occurred_at_ms`; `note` is optional. Origin and operator are
   not caller fields.
4. An identical request with the same key returns the original
   `adjustment_id` and `origin`, without adding a row or moving money. Request
   content includes all the fields above, including the optional note; it
   excludes the server's recording time. Omitting `note` and supplying `null`
   both mean no note.
5. Reusing a key with different content is refused as `correction_id_reused`,
   without changing the ledger. A missing, empty or oversized key is
   `malformed`.
6. Internal statement-settlement adjustments have no caller key and do not
   share this retry protocol. Existing unkeyed adjustments remain unchanged.

Tests: `correction_retry_moves_money_only_once`,
`correction_key_conflicting_content_is_refused_without_moving_money`,
`correction_keys_are_nonempty_and_bounded_in_utf8_bytes`,
`correction_key_migration_preserves_unkeyed_rows_and_enforces_partial_uniqueness`,
`correction_ingest_requires_key_and_replays_without_moving_money`.

## 2. Statements

1. Re-importing identical normalized statement content still appends import
   provenance and reconciliation attempts.
2. A line keeps its first comparable ledger snapshot rather than reading a
   new amount after settlement or correction.
3. Within a statement, an event's ledger amount is attributed to one line
   only; other lines referring to that event receive zero ledger attribution.
   The first eligible line settles the event, and later lines remain
   `already-settled`; their vendor amounts are not combined into a new
   settlement.
4. A uniquely correlated event in a different currency is
   `currency_mismatch`, including an event already settled before import.
   Reconciliation shows its actual ledger currency and does not subtract it
   from the vendor amount.
5. A target with no currency is `unpriced_target`, also without a difference
   amount. An initially unpriced line can settle on a later re-import after
   the event is priced.

The line match outcomes are a closed set: `unmatched`, `settled`,
`ambiguous`, `already-settled`, `unpriced_target`, `currency_mismatch`.

Tests: `same_content_reimport_preserves_once_per_event_ledger_attribution`,
`reimport_preserves_pre_adjustment_snapshot_and_split_line_attribution`,
`multiple_lines_for_one_candidate_attribute_ledger_amount_to_first_only`,
`only_already_settled_correlations_record_already_settled_without_resettling`,
`settled_currency_mismatch_never_reports_a_cross_currency_difference`,
`currency_mismatch_and_unpriced_target_report_absent_differences`,
`same_content_reimport_reconciles_unpriced_target_after_pricing`.
