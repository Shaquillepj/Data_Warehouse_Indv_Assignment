#NYC Motor Vehicle Collisions ETL to BigQuery - Star Schema
#--------ENVIRONMENT SETUP--------#
from datetime import date

#Google Auth
from google.colab import auth
auth.authenticate_user()
import pandas as pd
from google.cloud import storage

# GCP ID
project_id = 'cis9440-indv-assignment'
storage_client = storage.Client(project=project_id)

# Specific Bucket
bucket_name = 'nyc_mvc-bucket'
bucket = storage_client.get_bucket(bucket_name)

#--------EXTRACTING DATA--------#
blob_name = 'nyc_crashes.csv'
blob = bucket.blob(blob_name)

# Download temp file
blob.download_to_filename('/tmp/data.csv')
df = pd.read_csv('/tmp/data.csv')


#--------TRANSFORMATION--------#
# keep only rows with a collision_id and crash_date/time
df = df.dropna(subset=["collision_id", "crash_date", "crash_time"])

# collision_id as integer
df["collision_id"] = pd.to_numeric(df["collision_id"], errors="coerce")
df = df.dropna(subset=["collision_id"])
df["collision_id"] = df["collision_id"].astype("Int64")

# parse crash datetime
dt = pd.to_datetime(df["crash_date"] + " " + df["crash_time"], errors="coerce")

# unified time pieces for dim_time
df["clock_time"] = dt.dt.time
df["hour"]       = dt.dt.hour.astype("Int64")
df["minute"]     = dt.dt.minute.astype("Int64")

# unified date pieces for dim_date
df["calendar_date"]  = dt.dt.date
df["year"]           = dt.dt.year
df["month"]          = dt.dt.month
df["day"]            = dt.dt.day.astype("Int64")
df["weekday"]        = dt.dt.day_name()

# standardize text fields
for col in ["borough", "zip_code"]:
    df[col] = df[col].astype(str).str.strip()

# replace nan
df["borough"] = df["borough"].replace({"nan": None, "": None}).str.title()

street_cols = ["on_street_name", "off_street_name", "cross_street_name"]
for col in street_cols:
    df[col] = (
        df[col]
        .astype(str)
        .str.strip()
        .replace({"nan": None, "": None})
        .str.upper()
    )

# numeric injury / fatality measures
measure_cols = [
    "number_of_persons_injured",
    "number_of_persons_killed",
    "number_of_pedestrians_injured",
    "number_of_pedestrians_killed",
    "number_of_cyclist_injured",
    "number_of_cyclist_killed",
    "number_of_motorist_injured",
    "number_of_motorist_killed",
]

# fill na for numeric columns with 0
for col in measure_cols:
    df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).round().astype("Int64")

# unify latitude / longitude
df["latitude"]  = pd.to_numeric(df["latitude"],  errors="coerce")
df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")

# drop collisions where all injury / death counts are zero
mask_all_zero = df[measure_cols].fillna(0).eq(0).all(axis=1)
df = df[~mask_all_zero].copy()

dim_date = (
    df[["calendar_date", "year", "month", "day", "weekday"]]
    .drop_duplicates(subset=["calendar_date"])
    .reset_index(drop=True)
)

# create dim tables
dim_date["date_key"] = dim_date.index + 1

dim_time = (
    df[["clock_time", "hour", "minute"]]
    .drop_duplicates(subset=["clock_time"])
    .reset_index(drop=True)
)

dim_time["time_key"] = dim_time.index

dim_location = (
    df[["borough", "zip_code"]]
    .drop_duplicates()
    .reset_index(drop=True)
)

dim_location["location_key"] = dim_location.index + 1

dim_street = (
    pd.concat([
        df[["on_street_name"]].rename(columns={"on_street_name": "street_name"}),
        df[["off_street_name"]].rename(columns={"off_street_name": "street_name"}),
        df[["cross_street_name"]].rename(columns={"cross_street_name": "street_name"}),
    ])
    .dropna(subset=["street_name"])
    .drop_duplicates()
    .reset_index(drop=True)
)

dim_street["street_key"] = dim_street.index + 1

df["vehicle_type_code1"] = (
    df["vehicle_type_code1"]
    .astype(str)
    .str.strip()
    .replace({"nan": None, "": None})
    .str.upper()
)

dim_vehicle_type = (
    df[["vehicle_type_code1"]]
    .dropna(subset=["vehicle_type_code1"])
    .drop_duplicates()
    .reset_index(drop=True)
    .rename(columns={"vehicle_type_code1": "vehicle_type"})
)

dim_vehicle_type["vehicle_type_key"] = dim_vehicle_type.index + 1

df["contributing_factor_vehicle_1"] = (
    df["contributing_factor_vehicle_1"]
    .astype(str)
    .str.strip()
    .replace({"nan": None, "": None})
    .str.upper()
)

dim_contributing_factor = (
    df[["contributing_factor_vehicle_1"]]
    .dropna(subset=["contributing_factor_vehicle_1"])
    .drop_duplicates()
    .reset_index(drop=True)
    .rename(columns={"contributing_factor_vehicle_1": "factor_name"})
)

dim_contributing_factor["factor_key"] = dim_contributing_factor.index + 1

# Handle Null Foreign Keys-------#

# vehicle type
unknown_vehicle = pd.DataFrame({
    "vehicle_type": ["UNKNOWN"],
    "vehicle_type_key": [0]
})
dim_vehicle_type = pd.concat([unknown_vehicle, dim_vehicle_type], ignore_index=True)

# contributing factor
unknown_factor = pd.DataFrame({
    "factor_name": ["UNKNOWN"],
    "factor_key": [0]
})
dim_contributing_factor = pd.concat([unknown_factor, dim_contributing_factor], ignore_index=True)

# street
unknown_street = pd.DataFrame({
    "street_name": ["UNKNOWN"],
    "street_key": [0]
})
dim_street = pd.concat([unknown_street, dim_street], ignore_index=True)


#define ingestion date
df["ingestion_date"] = pd.Timestamp("today").normalize()

#Attach Keys back to main DF
# date_key
df = df.merge(
    dim_date[["calendar_date", "date_key"]],
    on="calendar_date",
    how="left"
)

# time_key
df = df.merge(
    dim_time[["clock_time", "time_key"]],
    on="clock_time",
    how="left"
)

# location_key
df = df.merge(
    dim_location[["borough", "zip_code", "location_key"]],
    on=["borough", "zip_code"],
    how="left"
)

# street keys
df = df.merge(
    dim_street[["street_name", "street_key"]],
    left_on="on_street_name",
    right_on="street_name",
    how="left"
).rename(columns={"street_key": "on_street_key"}).drop(columns=["street_name"])

df = df.merge(
    dim_street[["street_name", "street_key"]],
    left_on="off_street_name",
    right_on="street_name",
    how="left"
).rename(columns={"street_key": "off_street_key"}).drop(columns=["street_name"])

df = df.merge(
    dim_street[["street_name", "street_key"]],
    left_on="cross_street_name",
    right_on="street_name",
    how="left"
).rename(columns={"street_key": "cross_street_key"}).drop(columns=["street_name"])

# vehicle_type_key
df = df.merge(
    dim_vehicle_type[["vehicle_type", "vehicle_type_key"]],
    left_on="vehicle_type_code1",
    right_on="vehicle_type",
    how="left"
).drop(columns=["vehicle_type"])

# factor_key
df = df.merge(
    dim_contributing_factor[["factor_name", "factor_key"]],
    left_on="contributing_factor_vehicle_1",
    right_on="factor_name",
    how="left"
).drop(columns=["factor_name"])


# Build Fact Table
fact_collision = df[[
    "collision_id",
    "vehicle_type_key",
    "factor_key",
    "date_key",
    "time_key",
    "location_key",
    "on_street_key",
    "cross_street_key",
    "off_street_key",
    "number_of_persons_injured",
    "number_of_persons_killed",
    "number_of_pedestrians_injured",
    "number_of_pedestrians_killed",
    "number_of_cyclist_injured",
    "number_of_cyclist_killed",
    "number_of_motorist_injured",
    "number_of_motorist_killed",
    "latitude",
    "longitude",
    "ingestion_date",
]].copy()

# surrogate key
fact_collision = fact_collision.reset_index(drop=True)
fact_collision["collision_key"] = fact_collision.index + 1

fact_collision = fact_collision[[
    "collision_key",
    "collision_id",
    "vehicle_type_key",
    "factor_key",
    "date_key",
    "time_key",
    "location_key",
    "on_street_key",
    "cross_street_key",
    "off_street_key",
    "number_of_persons_injured",
    "number_of_persons_killed",
    "number_of_pedestrians_injured",
    "number_of_pedestrians_killed",
    "number_of_cyclist_injured",
    "number_of_cyclist_killed",
    "number_of_motorist_injured",
    "number_of_motorist_killed",
    "latitude",
    "longitude",
    "ingestion_date",
]]


#--------DATA LOADING--------#
# BigQuery Set up
project_id = "cis9440-indv-assignment"
dataset_id = "nyc_mvc_star"
# !pip install pandas-gbq --quiet

from pandas_gbq import to_gbq

# Load Dimensions Table
to_gbq(
    dim_date,
    f"{dataset_id}.dim_date",
    project_id=project_id,
    if_exists="replace"
)

to_gbq(
    dim_time,
    f"{dataset_id}.dim_time",
    project_id=project_id,
    if_exists="replace"
)

to_gbq(
    dim_location,
    f"{dataset_id}.dim_location",
    project_id=project_id,
    if_exists="replace"
)

to_gbq(
    dim_street,
    f"{dataset_id}.dim_street",
    project_id=project_id,
    if_exists="replace"
)

to_gbq(
    dim_vehicle_type,
    f"{dataset_id}.dim_vehicle_type",
    project_id=project_id,
    if_exists="replace"
)

to_gbq(
    dim_contributing_factor,
    f"{dataset_id}.dim_contributing_factor",
    project_id=project_id,
    if_exists="replace"
)

#Load fact table
to_gbq(fact_collision,
    f"{dataset_id}.fact_collision",
    project_id=project_id,
    if_exists="replace"
)
