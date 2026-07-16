# ecommerce-adls-file-trigger-pipeline
##  Project Overview

This project builds an **event-driven e-commerce data pipeline** on Databricks to process multi-source data such as orders, customers, products, inventory, and shipping. Data is ingested from Databricks Volumes and processed through automated workflows triggered by JSON-based events.

The pipeline follows a **Medallion Architecture (Bronze, Silver, Gold)** using Delta Lake, enabling structured data transformation from raw ingestion to analytics-ready datasets. It incorporates **data validation, SCD Type 2 for historical tracking**, and automated file management with logging and monitoring.

The final outputs support key business insights, including **customer segmentation, product performance, and Customer Lifetime Value (CLV)**, demonstrating a scalable and production-ready data engineering solution.

```text
                ┌──────────────────────────────┐
                │   Azure Data Lake Storage    │
                │        (bronze-dev)          │
                │                              │
                │  customers_data              │
                │  orders_data                 │
                │  products_data               │
                │  inventory_data              │
                │  shipping_data               │
                │  trigger_data (JSON files)   │
                └─────────────┬────────────────┘
                              │
                              │  (File Upload)
                              ▼
                ┌──────────────────────────────┐
                │   Trigger Detection Logic    │
                │ (JSON-based event trigger)   │
                └─────────────┬────────────────┘
                              │
                              ▼
                ┌──────────────────────────────┐
                │   Databricks Workflows       │
                │ (Job Orchestration Layer)    │
                └─────────────┬────────────────┘
                              │
     ┌───────────────┬────────┼───────────────┬───────────────┐
     ▼               ▼        ▼               ▼               ▼
┌──────────┐  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐
│Customers │  │ Orders   │ │Products  │ │Inventory │ │Shipping  │
│ Ingestion│  │ Ingestion│ │ Ingestion│ │ Ingestion│ │ Ingestion│
└────┬─────┘  └────┬─────┘ └────┬─────┘ └────┬─────┘ └────┬─────┘
     └──────────────┴────────────┴────────────┴──────────────┘
                              │
                              ▼
                ┌──────────────────────────────┐
                │   Bronze Layer (Delta Lake)  │
                │   Raw Tables                 │
                └─────────────┬────────────────┘
                              │
                              ▼
                ┌──────────────────────────────┐
                │   Silver Layer               │
                │ - Data Validation            │
                │ - Schema Checks             │
                │ - Cleaning                  │
                └─────────────┬────────────────┘
                              │
                              ▼
                ┌──────────────────────────────┐
                │   Gold Layer                 │
                │ - SCD Type 2                │
                │ - Data Enrichment            │
                │ - CLV / KPIs                │
                └─────────────┬────────────────┘
                              │
                              ▼
                ┌──────────────────────────────┐
                │   Analytics / Dashboard      │
                └──────────────────────────────┘

## Data Pipeline Flow
```text
  1. Source Data Landing (ADLS - bronze-dev)
   ├── customers_data
   ├── orders_data
   ├── products_data
   ├── inventory_data
   ├── shipping_data

2. Trigger Event
   └── JSON file uploaded to trigger_data folder

3. Event Detection
   └── Databricks detects trigger file

4. Workflow Execution
   └── Databricks Workflow starts job

5. Parallel Data Ingestion
   ├── customers_stage_load
   ├── orders_stage_load
   ├── products_stage_load
   ├── inventory_stage_load
   ├── shipping_stage_load

6. Bronze Layer (Raw Delta Tables)
   └── Store raw ingested data

7. Data Validation (Silver Layer)
   ├── Schema validation
   ├── Null checks
   ├── Cross-table validation
   └── Data quality scoring

8. Data Transformation & Enrichment
   ├── Join datasets
   ├── Business rules
   ├── Derived columns

9. SCD Type 2 Processing
   └── Delta MERGE (historical tracking)

10. Gold Layer (Business Tables)
   ├── Customer segmentation
   ├── Product performance
   ├── CLV calculation
   └── KPI tables

11. Output / Consumption
   └── Analytics / Dashboard

  




   
