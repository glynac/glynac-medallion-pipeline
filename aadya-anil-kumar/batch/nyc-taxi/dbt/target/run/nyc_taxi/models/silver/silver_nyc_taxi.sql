
  
    
    
    
        
         


        
  

  insert into `medallion`.`silver_nyc_taxi__dbt_backup`
        ("VendorID", "pickup_datetime", "dropoff_datetime", "passenger_count", "trip_distance", "PULocationID", "DOLocationID", "payment_type", "fare_amount", "tip_amount", "total_amount", "trip_duration_minutes")

SELECT
    VendorID,
    tpep_pickup_datetime AS pickup_datetime,
    tpep_dropoff_datetime AS dropoff_datetime,
    toInt32(passenger_count) AS passenger_count,
    trip_distance,
    toInt32(PULocationID) AS PULocationID,
    toInt32(DOLocationID) AS DOLocationID,
    toInt32(payment_type) AS payment_type,
    fare_amount,
    tip_amount,
    total_amount,
    dateDiff('minute', tpep_pickup_datetime, tpep_dropoff_datetime) AS trip_duration_minutes
FROM medallion.bronze_nyc_taxi
WHERE
    trip_distance > 0
    AND fare_amount > 0
    AND passenger_count > 0
    AND tpep_pickup_datetime >= '2024-01-01'
    AND tpep_pickup_datetime < '2024-02-01'
  