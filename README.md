# AiCore Multinational Retail Data Centralisation Project

Collate and analyse data from various sources, to create a local, centralised PostgreSQL sales database which acts as a single source of truth for sales data. Query the data to extract business metrics.

## IMPORTANT

The following YAML files are expected to exist in your root directory:  
db_creds_sales_data.yaml  
db_creds.yaml

These files contain credentials for connecting to the local PostgreSQL database, and the AWS RDS database containing orders data, respectively. The YAML files should contain the following keys:
RDS_HOST
RDS_PASSWORD
RDS_USER
RDS_DATABASE
RDS_PORT

## 1. Extract and clean source data

Extract data from various sources (API, JSON, PDF, RDS, S3); load each dataset into a Pandas Dataframe; clean the data; identify and remove invalid records; load each dataset to a table on the local PostgreSQL database.
- User details and order details sourced from PostgreSQL database hosted on AWS RDS; loaded to __sales_data.dim_users__ and __sales_data.orders_table__
- Credit card details sourced from PDF; loaded to __sales_data.dim_card_details__
- Store details sourced from API; loaded to __sales_data.dim_store_details__
- Product details sourced from S3; loaded to __sales_data.dim_products__
- Date events sourced from JSON; loaded to __sales_data.dim_date_times__

## 2. Create the database schema

Develop the star-based schema of the PostgreSQL database, ensuring that columns have the correct data types. Use SQL (executed via SQLAlchemy) to:
- Update data types for selected columns
- Derive columns using CASE/WHEN: e.g. create weight class column based on weight
- Set primary keys and foreign keys

## 3. Query the data

Use SQL to query the PostgreSQL database and create a report of business metrics, answering questions such as:
- Which locations have the most stores?
- Which months produce the highest sales?
- What percentage of sales come from each store type?
- How quickly is the company making sales? (average time interval between consecutive sales, grouped by year)

