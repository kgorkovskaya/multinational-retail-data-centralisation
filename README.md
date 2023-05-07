# AiCore Multinational Retail Data Centralisation Project

Collate and analyse data from various sources, to create a centralised sales database which acts as a single source of truth for sales data. Query the data to extract business metrics.

## 1. Create local database

- Create __sales_data__ database in PgAdmin.

## 2. Extract and clean source data

- Load user details from PostgreSQL database hosted on AWS into a Pandas DataFrame. Clean the data; remove records where a valid name is not present. Load to __sales_data.dim_users__
- Load credit card details from PDF file. Clean the data; remove records where valid card details are not present. Load to __sales_data.dim_card_details__
- Load store details from API. Clean the data; remove invalid/null records. Load to __sales_data.dim_store_details__


