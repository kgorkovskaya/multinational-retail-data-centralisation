# AiCore Multinational Retail Data Centralisation Project

Collate and analyse data from various sources, to create a centralised sales database which acts as a single source of truth for sales data. Query the data to extract business metrics.

## 1. Create local database

- Create __sales_data__ database in PgAdmin.

## 2. Extract and clean source data

Extract data from various sources; load each dataset into a Pandas Dataframe; standardize; identify and remove invalid records; load each dataset to a table on a local PostgreSQL database.
- User details and order details sourced from PostgreSQL database hosted on AWS; loaded to __sales_data.dim_users__ and __sales_data.orders_table__
- Credit card details sourced from PDF file; loaded to __sales_data.dim_card_details__
- Store details sourced from API; loaded to __sales_data.dim_store_details__
- Product details sourced from S3; loaded to __sales_data.dim_products__
- Date events sourced from JSON; loaded to __sales_data.dim_date_times__

## 3. Create the database schema

Develop the star-based schema of the database, ensuring that columns have the correct data types. Use SQL (executed via SQLAlchemy) to:
- Update data types for selected columns
- Derive columns using CASE/WHEN: e.g. create weight class column based on weight
- Set primary keys and foreign keys

