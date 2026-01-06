# Hotel Bookings Analytics – Snowflake

## Project Overview
This project demonstrates an end-to-end analytics pipeline built in Snowflake using a raw hotel bookings CSV dataset.  
The pipeline follows a Bronze → Silver → Gold architecture to support KPI-driven reporting.

## Architecture
Raw CSV → Bronze (Raw Load) → Silver (Cleaned & Validated) → Gold (Analytics)

## Key Features
- CSV ingestion using Snowflake stages and COPY INTO
- Data quality checks and cleaning
- Standardisation of customer, city, and booking status data
- Analytics-ready Gold tables for reporting

## Tech Stack
- Snowflake
- SQL
- Snowflake Dashboards

## KPIs
- Total Revenue
- Total Bookings
- Guests Count
- Revenue by City
- Booking Trends Over Time
