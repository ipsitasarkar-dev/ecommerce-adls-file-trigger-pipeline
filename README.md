# ecommerce-adls-file-trigger-pipeline
##  Project Overview

This project builds an **event-driven e-commerce data pipeline** on Databricks to process multi-source data such as orders, customers, products, inventory, and shipping. Data is ingested from Databricks Volumes and processed through automated workflows triggered by JSON-based events.

The pipeline follows a **Medallion Architecture (Bronze, Silver, Gold)** using Delta Lake, enabling structured data transformation from raw ingestion to analytics-ready datasets. It incorporates **data validation, SCD Type 2 for historical tracking**, and automated file management with logging and monitoring.

The final outputs support key business insights, including **customer segmentation, product performance, and Customer Lifetime Value (CLV)**, demonstrating a scalable and production-ready data engineering solution.

##   Tech Stack -

Databricks, PySpark, Delta Lake, Databricks Volumes, Databricks Workflows, GitHub

## Data Pipeline Flow

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
```
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
```

## 🚀 Key Features

-  **Event-Driven Data Pipeline**
  - Automatically triggers processing when a JSON file is uploaded to the `trigger_data` folder in ADLS.
  - Eliminates manual intervention and enables real-time-like ingestion.

-  **End-to-End Automated Workflow (Databricks)**
  - Fully orchestrated pipeline using Databricks Workflows.
  - Handles ingestion → validation → transformation → loading seamlessly.

-  **Parallel Data Processing**
  - Independent datasets (customers, orders, products, inventory, shipping) are processed in parallel.
  - Improves performance and reduces overall pipeline execution time.

-  **Medallion Architecture (Bronze, Silver, Gold)**
  - **Bronze**: Raw data ingestion (Delta tables)
  - **Silver**: Data validation, cleansing, and quality checks
  - **Gold**: Business-level aggregated tables for analytics

-  **Robust Data Validation Framework**
  - Schema enforcement
  - Null value checks
  - Deduplication
  - Cross-table consistency validation
  - Data quality scoring

-  **Slowly Changing Dimension (SCD Type 2)**
  - Maintains full historical tracking of data changes
  - Implemented using Delta Lake `MERGE` operations

-  **Business Logic & Data Enrichment**
  - Joins across multiple datasets
  - Derived columns and calculated metrics
  - Domain-specific transformations

-  **Analytics-Ready Gold Layer**
  - Precomputed KPIs and metrics
  - Customer segmentation
  - Product performance insights
  - Customer Lifetime Value (CLV)

-  **Scalable & Cloud-Native Architecture**
  - Built on Azure Data Lake Storage Gen2 + Databricks
  - Handles large-scale distributed data processing using Apache Spark

-  **Reliable & ACID-Compliant Storage**
  - Uses Delta Lake for:
    - ACID transactions
    - Schema evolution
    - Time travel (data versioning)

-  **Modular & Extensible Design**
  - Easy to add new datasets or pipelines
  - Reusable ingestion and transformation components

##  Data Model

This project uses **Unity Catalog (`ecommerce_dev_catalog`)** with a layered architecture in the `default` schema.

---

### 🟤 Stage Layer (Raw / Ingestion)

Raw data ingested directly from ADLS with minimal transformation.

- customers_stage  
- orders_stage  
- products_stage  
- inventory_stage  
- shipping_stage  

---

### ⚪ Target Layer (Cleaned & Processed)

Validated and transformed datasets used for downstream processing.

- customers_target  
- orders_target  
- enriched_orders  

---

### 🔴 Error & Logging Layer

Captures failed records and pipeline execution details.

- customers_errors  
- errors_stage  
- validation_results  
- processing_log  

---

### 🟡 Analytics Layer (Gold / Business Ready)

Final business-level aggregated tables for reporting and dashboards.

- customer_analytics  
- product_analytics  
- category_analysis  
- segment_analysis  
- seasonal_analysis  
- analytics_summary  

---

##  Data Flow Mapping

```text
ADLS (Source Data)
      │
      ▼
Stage Tables
(customers_stage, orders_stage, ...)
      │
      ▼
Target Tables
(customers_target, orders_target, enriched_orders)
      │
      ├──────────────► Error Tables (validation failures)
      │
      ▼
Analytics Tables (Gold Layer)
(customer_analytics, product_analytics, etc.)
```





 

   
