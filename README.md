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

- **Event-Driven Data Pipeline**
  - Automatically triggers processing when a JSON file is uploaded to ADLS.

- **End-to-End Automated Workflow**
  - Orchestrated using Databricks Workflows.

- **Parallel Data Processing**
  - Multiple datasets processed simultaneously for better performance.

- **Medallion Architecture (Bronze, Silver, Gold)**
  - Structured data processing layers for scalability and reliability.

- **Data Validation Framework**
  - Schema checks, null handling, deduplication, and quality validation.

- **SCD Type 2 Implementation**
  - Maintains historical data of customer data using Delta Lake MERGE operations.

- **Analytics-Ready Gold Layer**
  - Business KPIs, segmentation, and reporting tables.

- **Scalable Cloud Architecture**
  - Built using ADLS Gen2, Databricks, and Apache Spark.

- **Delta Lake Features**
  - ACID transactions, schema evolution, and time travel.


## 🧩 Data Model

This project uses **Unity Catalog (`ecommerce_dev_catalog`)** with a layered architecture in the `default` schema.

---

###  Stage Layer (Raw / Ingestion)

Raw data ingested directly from ADLS with minimal transformation.

- customers_stage  
- orders_stage  
- products_stage  
- inventory_stage  
- shipping_stage  

---

###  Target Layer (Cleaned & Processed)

Validated and transformed datasets used for downstream processing.

- customers_target  
- orders_target  
- enriched_orders  

---

###  Error & Logging Layer

Captures failed records and pipeline execution details.

- customers_errors  
- errors_stage  
- validation_results  
- processing_log  

---

###  Analytics Layer (Gold / Business Ready)

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





 

   
