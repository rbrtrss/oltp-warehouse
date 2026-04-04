# Data Contracts

This document defines the current data contract for the local pipeline in `oltp-warehouse`.

It is a practical v1 contract for portfolio and engineering use. It describes what the pipeline produces today, what downstream logic can rely on, and which changes would be considered breaking.

## Scope

This contract covers:

- bronze CDC parquet datasets under `data/raw/cdc/`
- silver parquet outputs under `data/silver/`
- the CDC watermark state file at `data/state/cdc_state.json`

This contract does not currently cover:

- observability artifacts under `data/observability/`
- service-level objectives or freshness SLAs
- delete tombstones or log-based CDC semantics
- formal schema versioning

## Contract Principles

- Bronze stores extracted source records in parquet batches.
- Silver stores one current row per business key.
- `updated_at` is required across all source domains and is part of both incrementality and silver deduplication behavior.
- Changes must preserve current dataset grain unless the contract is explicitly revised.

## Bronze Layer

Bronze datasets are written by `extract-cdc` into table-specific directories:

- `data/raw/cdc/accounts/`
- `data/raw/cdc/transactions/`
- `data/raw/cdc/transfers/`
- `data/raw/cdc/payments/`

Each extraction run may append a new parquet file for a table. Across files, the same business key may appear more than once as newer source versions are extracted.

### Bronze `accounts`

- Purpose: raw extracted account records from the synthetic OLTP source
- Grain: one extracted version per `account_id`
- Business key: `account_id`
- Required columns:
  - `account_id`
  - `customer_id`
  - `account_type`
  - `status`
  - `currency_code`
  - `balance`
  - `created_at`
  - `updated_at`

### Bronze `transactions`

- Purpose: raw extracted transaction records from the synthetic OLTP source
- Grain: one extracted version per `transaction_id`
- Business key: `transaction_id`
- Required columns:
  - `transaction_id`
  - `account_id`
  - `transaction_type`
  - `amount`
  - `status`
  - `description`
  - `created_at`
  - `updated_at`

### Bronze `transfers`

- Purpose: raw extracted transfer records from the synthetic OLTP source
- Grain: one extracted version per `transfer_id`
- Business key: `transfer_id`
- Required columns:
  - `transfer_id`
  - `source_account_id`
  - `destination_account_id`
  - `amount`
  - `status`
  - `created_at`
  - `updated_at`

### Bronze `payments`

- Purpose: raw extracted payment records from the synthetic OLTP source
- Grain: one extracted version per `payment_id`
- Business key: `payment_id`
- Required columns:
  - `payment_id`
  - `account_id`
  - `merchant_name`
  - `category`
  - `amount`
  - `status`
  - `created_at`
  - `updated_at`

### Bronze behavioral guarantees

- The first extraction run performs a full snapshot by table.
- Later extraction runs select rows where `updated_at > watermark`.
- Bronze parquet files must exist for all four source domains for local validation to pass.
- Bronze validation requires at least one readable parquet file and at least one row per table.

### Bronze non-guarantees

- No delete tombstones are produced today.
- No partitioning scheme is guaranteed beyond table-level directories.
- No automatic schema evolution framework exists.
- Cross-table event ordering is not guaranteed.

## Silver Layer

Silver datasets are produced by dbt and written as parquet files:

- `data/silver/silver_accounts.parquet`
- `data/silver/silver_transactions.parquet`
- `data/silver/silver_transfers.parquet`
- `data/silver/silver_payments.parquet`

Silver models read all bronze parquet files for a domain, cast fields explicitly, and keep one latest row per business key.

### Silver shared rules

- Grain: one current row per business key
- Deduplication rule:
  - partition by the business key
  - order by `updated_at desc, filename desc`
  - keep `row_number() = 1`
- `bronze_file_path` is preserved for lineage and traceability

### Silver `silver_accounts`

- Purpose: current analytics-ready account records
- Grain: one row per `account_id`
- Business key: `account_id`
- Output columns:
  - `account_id`
  - `customer_id`
  - `account_type`
  - `status`
  - `currency_code`
  - `balance`
  - `created_at`
  - `updated_at`
  - `bronze_file_path`
- Enforced tests:
  - `account_id` is `not_null`
  - `account_id` is `unique`

### Silver `silver_transactions`

- Purpose: current analytics-ready transaction records
- Grain: one row per `transaction_id`
- Business key: `transaction_id`
- Output columns:
  - `transaction_id`
  - `account_id`
  - `transaction_type`
  - `amount`
  - `status`
  - `description`
  - `created_at`
  - `updated_at`
  - `bronze_file_path`
- Enforced tests:
  - `transaction_id` is `not_null`
  - `transaction_id` is `unique`

### Silver `silver_transfers`

- Purpose: current analytics-ready transfer records
- Grain: one row per `transfer_id`
- Business key: `transfer_id`
- Output columns:
  - `transfer_id`
  - `source_account_id`
  - `destination_account_id`
  - `amount`
  - `status`
  - `created_at`
  - `updated_at`
  - `bronze_file_path`
- Enforced tests:
  - `transfer_id` is `not_null`
  - `transfer_id` is `unique`

### Silver `silver_payments`

- Purpose: current analytics-ready payment records
- Grain: one row per `payment_id`
- Business key: `payment_id`
- Output columns:
  - `payment_id`
  - `account_id`
  - `merchant_name`
  - `category`
  - `amount`
  - `status`
  - `created_at`
  - `updated_at`
  - `bronze_file_path`
- Enforced tests:
  - `payment_id` is `not_null`
  - `payment_id` is `unique`

### Silver non-guarantees

- No slowly changing dimension strategy is implemented.
- No additional semantic constraints are enforced beyond current validation and dbt tests.
- No gold-layer business metrics contract exists yet.

## CDC State Contract

The extraction watermark state is stored at:

- `data/state/cdc_state.json`

### Required shape

- The file must be valid JSON.
- The top-level value must be an object.
- The object must contain entries for:
  - `accounts`
  - `transactions`
  - `transfers`
  - `payments`
- Each value must be either:
  - an ISO-formatted timestamp string
  - `null`

### Behavioral meaning

- If a table watermark is missing, validation fails.
- If a table watermark is `null`, the next extraction behaves as if no successful watermark has been recorded for that table.
- On a successful extraction run, each table watermark is updated to the maximum extracted `updated_at` value for that table in the current run.
- If a table has no changed rows in a run, its previous watermark is preserved.

## Breaking and Non-Breaking Changes

### Breaking changes

- Removing any documented required bronze or silver column
- Renaming business keys
- Removing `updated_at`
- Changing bronze or silver grain
- Changing silver deduplication semantics from latest `updated_at` with `filename` tie-break
- Changing CDC state key names or timestamp format

### Non-breaking changes

- Adding nullable columns to bronze or silver datasets
- Adding documentation or observability around the datasets
- Adding stricter tests that do not change dataset shape or grain

## Current Enforcement

The current contract is enforced by a combination of:

- source DDL in the bootstrap step
- bronze validation for required columns, readable parquet files, row counts, and watermark state shape
- silver validation for required output columns and non-empty outputs
- dbt tests for `not_null` and `unique` on each silver business key

## Known Limitations

- The contract is descriptive and repo-local; it is not yet versioned independently.
- The pipeline does not currently emit delete events.
- The pipeline does not currently guarantee backward-compatible schema migration handling.
- Validation checks required columns and key tests, but not every business rule implied by field names.
