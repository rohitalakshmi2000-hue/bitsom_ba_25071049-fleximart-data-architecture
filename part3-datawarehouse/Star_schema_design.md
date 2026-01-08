Star Schema Design – FlexiMart Data Warehouse
Section 1: Schema Overview
Overview

The FlexiMart Data Warehouse is designed using a Star Schema model to support analytical reporting and business intelligence queries.
The star schema consists of a central fact table connected to multiple dimension tables. This structure allows fast querying, easy aggregation, and simplified reporting.

Fact Table: fact_sales
Grain

One record represents one product sold per order (line-item level).

This means:

If an order contains 3 different products → 3 rows in fact_sales

If the same product is bought twice → quantity reflects total units

Business Process

Sales transaction processing.

Measures (Numeric Facts)
Column Name	Description
quantity_sold	Number of units sold in the transaction
unit_price	Price per unit at time of sale
discount_amount	Discount applied to the sale
total_amount	Final calculated amount = (quantity × unit_price) − discount
Foreign Keys
Foreign Key	References
date_key	dim_date
product_key	dim_product
customer_key	dim_customer
Dimension Tables
1. dim_date

Purpose:
Provides time-based analysis such as daily, monthly, quarterly, and yearly trends.

Type: Conformed Dimension (used across multiple facts)

Attributes:

Column	Description
date_key (PK)	Surrogate key in YYYYMMDD format
full_date	Actual calendar date
day_of_week	Monday, Tuesday, etc.
month	Month number (1–12)
month_name	January, February, etc.
quarter	Q1, Q2, Q3, Q4
year	Calendar year
is_weekend	Boolean (True/False)
2. dim_product

Purpose:
Stores product-related descriptive information.

Attributes:

Column	Description
product_key (PK)	Surrogate key
product_id	Business product ID
product_name	Name of the product
category	Product category
brand	Brand name
unit_price	Standard product price
is_active	Indicates if product is active
3. dim_customer

Purpose:
Stores customer profile information for segmentation and analysis.

Attributes:

Column	Description
customer_key (PK)	Surrogate key
customer_id	Source system customer ID
customer_name	Full name
gender	Gender
city	City name
state	State
country	Country
signup_date	Customer registration date
Section 2: Design Decisions
1. Why Choose This Granularity?

The grain is one record per product per order, which allows:

Detailed transaction-level analysis

Accurate revenue calculations

Drill-down from yearly → monthly → daily → product level

Support for complex analytics like basket analysis

This is the most flexible and commonly used granularity in sales data warehouses.

2. Why Use Surrogate Keys Instead of Natural Keys?
Reason	Explanation
Stability	Business keys may change (email, product codes), surrogate keys do not
Performance	Integer keys join faster than string-based keys
History Tracking	Supports Slowly Changing Dimensions (SCD)
Data Integration	Handles multiple source systems with conflicting IDs
3. How This Design Supports Drill-Down and Roll-Up

Drill-Down (High → Low Detail):

Year → Quarter → Month → Day

Category → Product → Individual Sale

Roll-Up (Low → High Level):

Daily sales → Monthly revenue

Product sales → Category sales

City → State → Country

This is possible due to:

Structured date hierarchy in dim_date

Clean separation of facts and dimensions

Section 3: Sample Data Flow (End-to-End Example)
Source Transaction
Order ID: 101
Customer: John Doe
Product: Laptop
Quantity: 2
Unit Price: 50,000
Order Date: 15-Jan-2024

Step 1: Load Dimension Tables
dim_date
date_key: 20240115
full_date: 2024-01-15
day_of_week: Monday
month: 1
month_name: January
quarter: Q1
year: 2024
is_weekend: False

dim_product
product_key: 5
product_name: Laptop
category: Electronics
brand: Dell

dim_customer
customer_key: 12
customer_name: John Doe
city: Mumbai

Step 2: Load Fact Table
fact_sales
--------------------------------------
date_key       : 20240115
product_key    : 5
customer_key   : 12
quantity_sold  : 2
unit_price     : 50000
discount_amount: 0
total_amount   : 100000

