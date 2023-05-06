# AiCore Multinational Retail Data Centralisation Project

Collate and analyse data from various sources, to create a centralised sales database which acts as a single source of truth for sales data. Query the data to extract business metrics.

## 1. Extract and clean source data

- Load user details from PostgreSQL database hosted on AWS into a Pandas DataFrame
- Clean the data (identify and remove invalid names, dates, country names, country codes, phone numbers, and email addresses; remove records where a valid name is not present)
- Load to a new table on local PostgreSQL database


