
  
    
    
    
        
         


        
  

  insert into `medallion`.`gold_nyc_taxi_daily__dbt_backup`
        ("date", "total_trips", "avg_fare", "avg_trip_distance", "avg_duration_mins", "avg_passengers", "total_revenue")

SELECT
    toDate(pickup_datetime) AS date,
    COUNT(*) AS total_trips,
    AVG(fare_amount) AS avg_fare,
    AVG(trip_distance) AS avg_trip_distance,
    AVG(trip_duration_minutes) AS avg_duration_mins,
    AVG(passenger_count) AS avg_passengers,
    SUM(total_amount) AS total_revenue
FROM `medallion`.`silver_nyc_taxi`
GROUP BY date
ORDER BY date
  