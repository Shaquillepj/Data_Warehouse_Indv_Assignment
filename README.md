# NYC Collision Data Warehouse- Assignment 1 & 2

## Project Overview
This repository contains the scripts and documentation for Assignment #1 and #2, which  includes sourcing raw data via API, storing it in Google Cloud Storage, and modeling it for a Data Warehouse ingestion. 

## Data Sourcing
* **Dataset:** NYC Motor Vehicle Collision Data:(https://data.cityofnewyork.us/Public-Safety/Motor-Vehicle-Collisions-Crashes/h9gi-nx95/about_data)
* **Sourcing Method:** Web API
* **Data Dictionary:** `docs/data_dictionary_mvc.pdf`
* **Exported Data:** `docs/mvc_nyc_crashes_raw.csv`

## Storage
* **Raw Data Storage:** Google Cloud Storage
* **Path:** `https://storage.googleapis.com/nyc_mvc-bucket/nyc_crashes.csv`


## Modeling
* **Target Database:** Google BigQuery
* **Schema:** Star Schema  (fact_collision, dim_time, dim_location, dim_street, dim_date, dim_contributing_factor, dim_vehicle_type)
* **Model Diagram:** The complete ERD is available in `docs/mvc_schema.pdf`

## Script
| Script File | Purpose 
| :--- | :--- 
| `script/nyc_mvc_etl.py` | **ETL Process:** Reads staging data from Google Cloud Storage, transform data and loads into Google BigQuery


## Data Serving
* **Tableau Dashboard:** https://public.tableau.com/views/nyc_mvc_dash/mvc_data_serving?:language=en-US&publish=yes&:sid=&:redirect=auth&:display_count=n&:origin=viz_share_link
* **Dashhboard Export:** `docs/mvc_data_serving_dash.png`


