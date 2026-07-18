# ecommerce-incremental-load

A pragmatic event-driven data pipeline for ecommerce CSV feeds, built to run in Azure Databricks with Delta Lake staging, validation, enrichment, and SCD2 history.

This repo is the handoff document for a pipeline that is meant to be run, observed, and debugged by another engineer. It is not marketing copy — it is a map of how the pipeline is wired, where data quality is enforced, and what to watch for after a run.

---

## What this repo is for

It exists because raw ecommerce CSV feeds are arriving into Azure Blob Storage and those files need to be turned into analytics-ready tables without losing history.

What it solves:

- captures source data as soon as a file lands
- isolates raw ingestion from business logic
- validates cross-feed consistency before enrichment
- preserves history with SCD2 instead of overwriting target rows
- keeps failure evidence in Delta tables so bad rows are visible

This is the kind of pipeline I would hand over with a “run it, then inspect `processing_log` and `validation_results` first” note attached.

---

## High-level flow

           ┌─────────────────────┐
           │  CSVs arrive in Blob │
           │  (orders, customers, │
           │   products, inventory,
           │   shipping)          │
           └─────────┬───────────┘
                     │
                     │ blob-created event
                     ▼
      ╭──────────────────────────────────╮
      │ Event Grid → Storage Queue       │
      │ (single event, buffered message) │
      ╰──────────────────────────────────╯
                     │
                     │ queue poll / task trigger
                     ▼
      ╭──────────────────────────────╮
      │ Databricks loader tasks       │
      │ 01–05 stage_load notebooks    │
      ╰──────────────────────────────╯
              │     │     │     │     │
              │     │     │     │     │
              ▼     ▼     ▼     ▼     ▼
    ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐
    │ orders  │ │customers│ │products │ │inventory│ │shipping │
    │ stage   │ │ stage   │ │ stage   │ │ stage   │ │ stage   │
    └─────────┘ └─────────┘ └─────────┘ └─────────┘ └─────────┘
              │
              │ all feeds landed and typed
              ▼
      ╭──────────────────────────────╮
      │ 06_data_validation           │
      │ cross-feed integrity checks  │
      ╰──────────────────────────────╯
              │
              ▼
      ╭──────────────────────────────╮
      │ 07_data_enrichment           │
      │ analytics-ready joins +      │
      │ derived attributes           │
      ╰──────────────────────────────╯
              │
              ▼
      ╭──────────────────────────────╮
      │ 08_final_merge_operation     │
      │ SCD2 merge to target         │
      ╰──────────────────────────────╯

Notes:

- `01`–`05` are ingestion and staging work. They are intentionally lightweight and isolated.
- `06` validates across feeds. It must run after all staged tables exist.
- `07` only runs once the data is structurally correct and consistent enough for analytics.
- `08` is a write-heavy merge stage; it should be the last step.

---

## What each notebook does

### `01_orders_stage_load.ipynb`

Loads orders from blob CSV into a Delta stage table.

Key behaviour:

- explicit schema, no schema inference
- `batch_id` and `processed_timestamp` added
- valid rows written to `orders_stage`
- invalid rows written to an error table with reason codes
- source file moved to `archive/`

Why this exists:

The staging layer is the first point where raw CSV becomes structured data. It also ensures bad rows are quarantined rather than silently dropped.

### `02_customers_stage_load.ipynb`

Loads customer records and enriches them with simple lifecycle segments.

What it checks:

- `customer_id`, `email`, `phone` must be present
- date of birth cannot be in the future
- email pattern sanity

What it derives:

- age segment (Gen Z / Millennial / Gen X / Boomer+)
- customer lifecycle stage (New / Active / Established)

### `03_products_stage_load.ipynb`

Parses product metadata and computes stock/lifecycle indicators.

What it does:

- validates `product_id` and `price`
- parses dimensions from `LxWxH`
- computes volume, density, and price segment
- flags discontinued or low-stock products

### `04_inventory_stage_load.ipynb`

Ingests inventory snapshots and enforces stock consistency.

Checks include:

- `inventory_id` is present
- quantities are non-negative
- stock utilization is calculated
- overdue audits are flagged

### `05_ShippingData_Stage_Load.ipynb`

Loads shipping records and compares actual delivery performance.

Enrichments include:

- on-time / slightly delayed / delayed labels
- cost per kilogram
- validation of shipping cost and weight

### `06_data_validation.ipynb`

This notebook is the most important quality gate.

It does not just validate one feed. It validates the relationship between all five feeds.

Checks include:

- orphan orders / missing customers
- orders linked to missing products
- shipping records without matching orders
- product records never ordered
- price mismatches versus product list prices
- time-of-day and order amount sanity checks

It writes results to `validation_results` and publishes a pass/fail flag for the next task.

### `07_data_enrichment.ipynb`

Joins the five staged feeds into analytics-ready outputs.

Outputs include:

- `enriched_orders`
- `customer_analytics`
- `product_analytics`

This notebook is where business context is added: margins, lifecycle labels, customer segments, product performance tiers.

### `08_final_merge_operation.ipynb`

Performs the SCD2 merge into target tables.

The process is:

1. detect changed rows
2. expire current versions (`is_current = false`)
3. append new versions with `effective_date`
4. keep full history in the same table

This is the final commit point for the pipeline.

---

## Why this design

I chose explicit staging because it makes debugging and retries easier.

If a feed fails in `02`, I can fix the CSV and re-run only that notebook. I don't have to rerun the entire pipeline from raw file ingestion.

Validation is separate because the cross-feed checks are not the same thing as row-level schema validation. A row can be internally valid and still be bogus if it points at the wrong customer or product.

The merge stage preserves row history instead of overwriting it, which is the safer choice when dimensions change over time.

---

## Architecture sketch

           ┌─────────────────────────────┐
           │ Azure Blob Storage (landing) │
           └───────────┬─────────────────┘
                       │ CSV file arrival
                       ▼
            ┌───────────────────────────┐
            │ Event Grid / Storage Queue │
            │ buffer the file event      │
            └───────────┬───────────────┘
                        │ queue pull
                        ▼
       ╭────────────────────────────────────────╮
       │ Databricks job starts loader notebooks │
       │ 01–05: one notebook per feed          │
       ╰────────────────────────────────────────╯
                    │  │  │  │  │
                    │  │  │  │  │
                    ▼  ▼  ▼  ▼  ▼
              load -> stage tables and archives
                    │
                    ▼
       ╭──────────────────────────────────╮
       │ 06_data_validation                │
       │ cross-feed integrity checks      │
       ╰──────────────────────────────────╯
                    │
                    ▼
       ╭──────────────────────────────────╮
       │ 07_data_enrichment                │
       │ build analytics-ready outputs    │
       ╰──────────────────────────────────╯
                    │
                    ▼
       ╭──────────────────────────────────╮
       │ 08_final_merge_operation          │
       │ SCD2 history writes               │
       ╰──────────────────────────────────╯

Notes on the sketch:

- the blob event does not go directly to Databricks; it is buffered in a queue first.
- stage tables are the first durable checkpoint.
- validation is the first place where all feeds are compared.
- enrichment is where analytical context is created.
- merge is the final durable write.

---

## Key diagrams and screenshots

The image assets are stored alongside this README in `files/`, with additional visual references in the sibling `images/` directory.

### Architecture reference

![event-driven architecture](./files/architecture-diagram.svg)

![Azure portal reference](./files/original-azure-diagram.jpg)

![alternative architecture sketch](./files/architecture_diagram.jpg)

### Pipeline flow and runtime

![pipeline DAG](./files/pipeline-dag.svg)

![Databricks job graph](./files/databricks-job-graph.png)

![workflow visual](./files/worklflow.png)

### Execution evidence

![enrichment summary](./files/analytics-summary-run.png)

![final merge output](./files/final-merge-run.png)

![final output](./files/final_output.png)

![metric screenshot](./files/images/metric.png)

### Security and access

![IAM role assignments](./files/iam-role-assignments.jpeg)

---

## Operations and troubleshooting

What to check first after a failed run:

- `processing_log` for the notebook that failed
- `validation_results` for high-severity rows
- whether the source CSV was archived successfully
- whether the queue message was processed more than once
- whether the target table merge step has partially updated rows

A common mistake:

The job currently logs validation results but does not stop on medium or low severity automatically. That means a run can continue even when the data is not ideal, so the engineer should inspect the validation table before trusting downstream analytics.

If you need a quick recovery path:

- fix the source file in landing
- re-run only the failed loader notebook if the issue is at ingestion
- if the issue is a cross-feed mismatch, fix the table or source file and re-run `06_data_validation` then `07_data_enrichment`
- only re-run `08_final_merge_operation` when the upstream feed is stable and the target state is understood

---

## File layout from this README

- `01_orders_stage_load.ipynb`
- `02_customers_stage_load.ipynb`
- `03_products_stage_load.ipynb`
- `04_inventory_stage_load.ipynb`
- `05_ShippingData_Stage_Load.ipynb`
- `06_data_validation.ipynb`
- `07_data_enrichment.ipynb`
- `08_final_merge_operation.ipynb`
- `architecture-diagram.svg`
- `pipeline-dag.svg`
- `original-azure-diagram.jpg`
- `databricks-job-graph.png`
- `analytics-summary-run.png`
- `final-merge-run.png`
- `iam-role-assignments.jpeg`

---

## What the next engineer should take away

- The most important tables to inspect are `processing_log` and `validation_results`.
- The notebook order is intentional: stage -> validate -> enrich -> merge.
- This repo is structured for handoff, not for experimentation. If you change the pipeline order or merge semantics, document it here.
- The next improvement would be turning `validation_results` into a gating mechanism rather than passive logging.

*This README is written as a practical handoff note, not a polished external summary.*
