FlexiMart Data Architecture Project ;

Student Name: Vedavalli Rohitha Lakshmi Student ID: bitsom_ba_25071049 Email: rohitalakshmi2000@gmail.com Course: Business Analytics with Gen & Agentic AI | Submission Date: 08-01-2026.

Project Overview

The FlexiMart Data Architecture Project implements an end-to-end data solution for a retail business environment. The project focuses on ingesting raw transactional and product data, performing data cleaning and transformation using Python-based ETL pipelines, and storing the processed data in a relational database. In addition, a NoSQL-based product catalog is implemented using MongoDB, and an analytical data warehouse is designed using a star schema to support business intelligence queries.

FlexiMart_Data_Architecture/ ├── part1-database-etl/ │ ├── etl_pipeline.py │ ├── schema_documentation.md │ ├── business_queries.sql │ └── data_quality_report.txt │ ├── part2-nosql/ │ ├── nosql_analysis.md │ ├── mongodb_operations.js │ └── products_catalog.json │ ├── part3-datawarehouse/ │ ├── star_schema_design.md │ ├── warehouse_schema.sql │ ├── warehouse_data.sql │ └── analytics_queries.sql │ └── README.md

Technologies Used Programming & Libraries ; Python 3.x pandas SQLAlchemy mysql-connector-python Databases ; MySQL 8.0 – Transactional database and data warehouse MongoDB 6.0 – NoSQL document database Tools ; Visual Studio Code MySQL Workbench / MySQL Client MongoDB Compass / mongosh

Setup Instructions 4.1 Relational Database Setup;

Create transactional and data warehouse databases
mysql -u root -p -e "CREATE DATABASE fleximart;" mysql -u root -p -e "CREATE DATABASE fleximart_dw;"

4.2 Part 1 – ETL Pipeline Execution; The ETL pipeline performs the following steps: Reads raw CSV files containing customers, products, and sales data. Cleans and standardizes inconsistent data formats. Handles missing values and removes duplicate records. Generates surrogate keys. Loads clean data into MySQL tables. python part1-database-etl/etl_pipeline.py

4.3 Execute Business Queries; mysql -u root -p fleximart < part1-database-etl/business_queries.sql Business questions addressed include: Customers with multiple purchases and high spending Product categories generating high revenue Monthly sales and cumulative revenue trends.

4.4 Part 3 – Data Warehouse Setup; mysql -u root -p fleximart_dw < part3-datawarehouse/warehouse_schema.sql mysql -u root -p fleximart_dw < part3-datawarehouse/warehouse_data.sql mysql -u root -p fleximart_dw < part3-datawarehouse/analytics_queries.sql

MongoDB Setup (Part 2 – NoSQL); mongosh < part2-nosql/mongodb_operations.js

Key Learnings; Developed practical experience building an end-to-end ETL pipeline using Python and SQL. Understood when to use relational databases, NoSQL databases, and data warehouses. Learned how MongoDB supports flexible schemas and nested data structures. Gained hands-on experience designing star schemas for analytical workloa.

Challenges Faced;

Handling inconsistent and incomplete source data Solution: Implemented validation rules, default values, and deduplication logic in the ETL process. Database connection and configuration issues. Solution: Resolved authentication, port, and driver issues by verifying services and configurations. Designing efficient fact–dimension relationships. Solution: Carefully mapped business metrics to fact tables and descriptive attributes to dimension tables.
