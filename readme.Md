# Modern Data Warehouse with dbt (Medallion Architecture)

## 📌 Project Overview
This project demonstrates a production-grade batch data pipeline using the Medallion Architecture (Bronze, Silver, Gold layers) and **dbt (data build tool)**. It extracts raw e-commerce data, loads it into a cloud data warehouse, and transforms it into business-ready analytical tables.



## 🎯 Objectives
* **Data Modeling:** Implement a scalable Medallion Architecture.
* **Data Quality:** Enforce data integrity using dbt tests (unique, not_null, accepted_values).
* **Version Control:** Maintain data transformations as code (Analytics Engineering).
* **Documentation:** Auto-generate data lineage graphs and data dictionaries.

## 🛠️ Tech Stack
* **Data Warehouse:** [Google BigQuery / Snowflake / PostgreSQL]
* **Transformation Engine:** dbt Core
* **Language:** SQL, Python (for initial load)
* **Containerization:** Docker (optional, for running dbt)

## 📂 Project Structure

```text
├── data/                      # Raw seed data (CSV)
├── dbt_project/               # Main dbt project folder
│   ├── models/                # SQL transformation models
│   │   ├── bronze/            # Raw data views (Source)
│   │   ├── silver/            # Cleaned, conformed, and deduplicated data
│   │   └── gold/              # Business-level aggregations and reporting tables
│   ├── tests/                 # Custom singular data tests
│   ├── macros/                # Reusable SQL functions
│   ├── dbt_project.yml        # dbt configuration file
│   └── packages.yml           # dbt dependencies (e.g., dbt-utils)
├── scripts/                   # Python scripts for data loading
├── requirements.txt           # Python dependencies
└── README.md
