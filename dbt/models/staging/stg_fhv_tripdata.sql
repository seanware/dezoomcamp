{{ config(materialized='view') }}

with tripdata as (
    select *, 
        row_number() over(partition by dispatching_base_num, pickup_datetime) as rn
  from {{ source('staging', 'fhv_nonpartitioned_tripdata')}}
    where dispatching_base_num is not null 
)


select 
    {{ dbt_utils.surrogate_key(['dispatching_base_num', 'pickup_datetime']) }} as tripid,
    cast(dispatching_base_num as STRING) as dispatching_base_num,	
    cast(pickup_datetime as TIMESTAMP) as pickup_datetime,	
    cast(dropoff_datetime as TIMESTAMP) as dropoff_datetime,	
    cast(PULocationID as INTEGER) as PULocationID,	
    cast(DOLocationID as INTEGER) as DOLocationID,	
    cast(SR_Flag as	INTEGER) as SR_Flag
from tripdata	
where rn = 1

{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}