# Modern Data Warehouse with dbt (Medallion Architecture)

## Project Overview
This project demonstrates a production-grade batch data pipeline using the Medallion Architecture (Bronze, Silver, Gold layers) and **dbt (data build tool)**. It extracts raw e-commerce data, loads it into a cloud data warehouse, and transforms it into business-ready analytical tables.



## Objectives
* **Data Modeling:** Implement a scalable Medallion Architecture.
* **Data Quality:** Enforce data integrity using dbt tests (unique, not_null, accepted_values).
* **Version Control:** Maintain data transformations as code (Analytics Engineering).
* **Documentation:** Auto-generate data lineage graphs and data dictionaries.

## Tech Stack
* **Data Warehouse:** [Google BigQuery / Snowflake / PostgreSQL]
* **Transformation Engine:** dbt Core
* **Language:** SQL, Python (for initial load)
* **Containerization:** Docker (optional, for running dbt)

## Project Structure

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


How to Run the Project
1. Prerequisites
Python 3.9+ installed

Access to [Your chosen Data Warehouse] credentials

Git

2. Setup the Environment
Clone the repository and install the required dependencies:

Bash
git clone [https://github.com/yourusername/your-repo-name.git](https://github.com/yourusername/your-repo-name.git)
cd your-repo-name
python -m venv venv
source venv/bin/activate  # On Windows use `venv\Scripts\activate`
pip install -r requirements.txt
3. Initialize dbt
Navigate to the dbt project folder and configure your connection profile:

Bash
cd dbt_project
dbt debug  # Verifies connection to the Data Warehouse
4. Run the Pipeline
Execute the models in order (Bronze -> Silver -> Gold):

Bash
dbt seed   # Loads raw CSV data into the warehouse (if using seeds)
dbt run    # Executes the SQL models to build tables/views
dbt test   # Runs data quality tests
5. Generate Documentation
Generate and serve the documentation and data lineage graph:

Bash
dbt docs generate
dbt docs serve
