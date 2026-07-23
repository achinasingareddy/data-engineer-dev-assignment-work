# Azure Medallion Data Engineering Assignment

## University Chapters Data Product

### Author

China Singaiah Annapureddy

---

# Project Overview

This project demonstrates an end-to-end Azure Medallion Architecture pipeline that ingests University Chapters data from a public ArcGIS REST API, performs data quality validation, and publishes a trusted Gold data product.

The solution follows modern Azure Data Engineering practices using Python and PySpark.

The pipeline consists of:

```
ArcGIS REST API --> Bronze Layer  --> Silver Layer --> Gold Layer

# Objective

Build a reusable data product containing active university chapters for the following states:

- California (CA)
- Oregon (OR)
- Washington (WA)

The published Gold dataset is designed for analytics and reporting.

---

# Technology Stack

Technology    ---    Purpose 
Python        ---    API Ingestion
PySpark       ---    Data Transformation
Parquet       ---    Storage
GitHub        ---    Source Control
REST API      ---    Data Source 
PyTest        ---    Unit Testing



# Running the Project

1. Create databricks free edition account
2. Go to Workspace --> users (right side panel) --> your email id --> click on create(right side panel) --> git folder
3. Add Repository URL https://github.com/achinasingareddy/data-engineer-dev-assignment-work.git and provider as github
4. Under Catalog section --> create new catalog with name "dev_engineering_catalog"
5. create new schema - dev_engineering_schema under "dev_engineering_catalog"
6. create volume dev_engineering_volume to store files under dev_engineering_schema

# Execute Pipeline

### Step 1

Data Ingestion 

Run  src/ingestion.py from databricks workspace


### Step 2

Bronze to Silver

Run src/bronze_to_silver.py from databricks workspace


### Step 3

Silver to Gold

Run src/silver_to_gold.py from databricks workspace


# Output

```
Bronze --> Silver    --> Gold
       --> Quarantine

# Unit Testing

Run

```bash
pytest tests/
```

---

# Logging

Each execution logs

```
Rows In

Rows OK

Rows Warning

Rows Quarantine

Execution Time
```

---

# Data Product Contract

Documentation available under

```
contract/
```

Contains

- Data Product Contract
- Schema Definition
- Data Quality Rules

---

# Assumptions

- Source API is publicly accessible.
- Only CA, OR, and WA data is processed.
- Empty OR/WA datasets are considered valid.
- Pipeline fails if the API returns no records.

# Future Improvements

If this solution were implemented in production, the following enhancements would be added:

- Azure Data Factory orchestration
- Azure Key Vault integration
- Delta Lake MERGE operations
- Incremental data loads
- Partitioned Bronze layer
- Azure Monitor and Log Analytics
- Unity Catalog integration
- Data lineage
- Automated scheduling
- Email/Teams notifications
- Data quality dashboard

# Design Decisions

- Medallion Architecture for data lifecycle management.
- Separate Quarantine layer to isolate invalid records.
- Gold layer contains only trusted data.
- Idempotent publishing using overwrite (can be replaced with Delta MERGE in production).
- Modular Python scripts for maintainability.
- Data quality rules implemented during Bronze → Silver transformation.

# Conclusion

This project demonstrates a complete Medallion Architecture implementation using Python and PySpark to build a reusable, consumer-ready data product. It follows Azure Data Engineering best practices with a clear separation of Bronze, Silver, Gold, and Quarantine layers, automated data quality validation, and comprehensive project documentation.
