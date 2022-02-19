{{ config(materialized='table') }}



with fhv_tripdata as (
    select * from {{ ref('stg_fhv_tripdata') }}
)

select 
    dispatching_base_num,	
    pickup_datetime,	
    dropoff_datetime,	
    PULocationID,	
    DOLocationID,	
    SR_Flag
from fhv_tripdata

