-- Create external table for FHV data
CREATE OR REPLACE EXTERNAL TABLE `de-zoomcamp-338916.trips_data_all.external_fhv_tripdata`
OPTIONS (
    format = 'parquet',
    uris = ["gs://dtc_data_lake_de-zoomcamp-338916/raw/fhv_tripdata_2019-*.parquet"]
);

--count for fhv vehicles
--Ans 42084899
SELECT COUNT(*) AS vehicles 
FROM de-zoomcamp-338916.trips_data_all.external_fhv_tripdata;

--Distinct dispatching_base_num for FHV 2019
--Ans 792
SELECT COUNT(DISTINCT(dispatching_base_num)) 
FROM de-zoomcamp-338916.trips_data_all.external_fhv_tripdata;

--partition and/or cluster by dropoff_datetime and dispatching_base_num
--partition by dropoff_datetime , cluster by dispatching_base_num

-- Creating a partition and cluster table
CREATE OR REPLACE TABLE de-zoomcamp-338916.trips_data_all.fhv_tripdata_partitioned_clustered
PARTITION BY DATE(dropoff_datetime)
CLUSTER BY dispatching_base_num AS
SELECT * FROM de-zoomcamp-338916.trips_data_all.external_fhv_tripdata;

-- Count trips between 2019/01/01 + 2019/03/31 dispatching_base_num B00987
-- B02060, B02279
--Ans 26558  est. 400MB processed 141.8 MB
SELECT COUNT(*) AS trips
FROM de-zoomcamp-338916.trips_data_all.fhv_tripdata_partitioned_clustered
WHERE dropoff_datetime BETWEEN '2019-01-01' AND '2019-03-31' 
    AND dispatching_base_num IN ('B00987', 'B02060', 'B02279');

--
