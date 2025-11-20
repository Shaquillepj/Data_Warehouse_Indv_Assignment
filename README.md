# NYC Collision Data Warehouse--Assignment #1

## Project Overview
This repository contains the scripts and documentation for Assignment #1, which  includes sourcing raw data via API, storing it in Google Cloud Storage, and modeling it for a Data Warehouse ingestion. 

## Data Sourcing
* **Dataset:** NYC Motor Vehicle Collision Data:(https://data.cityofnewyork.us/Public-Safety/Motor-Vehicle-Collisions-Crashes/h9gi-nx95/about_data)
* **Sourcing Method:** Web API
* **Data Dictionary:** `docs/data_dictionary.pdf`
* **Exported Data:** `docs/nyc_crashes.csv`

## Storage
* **Raw Data Storage:** Google Cloud Storage
* **Path:** `gs://cis9440_indv_assignment/nyc_crashes.csv`


## Modeling
* **Target Database:** Google BigQuery
* **Schema:** Star Schema with Bridge Tables (Fact Collision, Dim Date, Dim Location
* **Model Diagram:** The complete ERD is available in `docs/erd.pdf`

## Scripts
| Script File | Purpose 
| :--- | :--- 
| `notebooks/data_pull.ipynb` | **Data Sourcing & Storage:** Extracts data using API and transforms it
| `sql/create_tables.sql` | **Data Modeling:** To create the Data Warehouse schema in BigQuery


