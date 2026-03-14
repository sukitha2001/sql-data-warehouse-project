# SQL Data Warehouse & Analytics Project 📊

Welcome to the **SQL Data Warehouse and Analytics Project**! 🚀

This repository demonstrates an end-to-end data warehousing and analytics solution. It showcases the complete data engineering lifecycle, from building a robust data warehouse using the Medallion Architecture to generating actionable business insights through SQL-based analytics.

---

## 🏗️ Data Architecture

This project implements the industry-standard **Medallion Architecture** to organize and process data efficiently:

![Data Architecture Diagram](docs/data_architecture.png)

1. **Bronze Layer (Raw Data)**: Ingests and stores raw data as-is from source systems (e.g., ERP and CRM CSV files) into the SQL database.
2. **Silver Layer (Cleansed Data)**: Focuses on data quality. This layer handles data cleansing, standardization, normalization, and resolving data quality issues to prepare it for analysis.
3. **Gold Layer (Business-Ready Data)**: Houses refined data modeled into a star schema (fact and dimension tables) optimized for fast analytical queries and BI reporting.

---

## 📖 Project Overview

This project highlights key skills in Data Engineering and Analytics:
* **Modern Data Warehousing:** Designing a scalable architecture (Bronze, Silver, Gold).
* **ETL Pipeline Development:** Extracting raw data, transforming it for consistency, and loading it into analytical models.
* **Data Modeling:** Building optimized star schemas tailored for business intelligence.
* **Data Analytics:** Writing advanced SQL queries to extract insights regarding customer behavior, product performance, and sales trends.

---

## 📂 Repository Structure

```text
sql-data-warehouse-project/
│
├── datasets/                           # Raw source datasets (ERP and CRM data)
├── docs/                               # Project documentation, architecture diagrams, and data dictionaries
├── scripts/                            # SQL scripts for ETL pipelines
│   ├── bronze/                         # DDL and ingestion scripts for raw data
│   ├── silver/                         # Data cleansing and transformation scripts
│   ├── gold/                           # Scripts for creating fact and dimension tables (Star Schema)
├── tests/                              # Data quality checks and testing scripts
├── README.md                           # Project overview
└── LICENSE                             # Repository license