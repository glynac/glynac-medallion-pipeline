-- Create medallion database
CREATE DATABASE IF NOT EXISTS medallion;

-- ── BRONZE LAYER ──────────────────────────────────────────────────────

-- Bronze: Raw weather data from NiFi
CREATE TABLE IF NOT EXISTS medallion.bronze_weather (
    ingested_at     DateTime DEFAULT now(),
    city            String,
    country         String,
    temperature     Float64,
    feels_like      Float64,
    humidity        Int32,
    wind_speed      Float64,
    weather_main    String,
    weather_desc    String,
    timestamp       DateTime
) ENGINE = MergeTree()
ORDER BY (city, timestamp);

-- Bronze: Raw NYC Taxi data from Airflow
CREATE TABLE IF NOT EXISTS medallion.bronze_nyc_taxi (
    ingested_at             DateTime DEFAULT now(),
    VendorID                Nullable(Int32),
    tpep_pickup_datetime    Nullable(DateTime),
    tpep_dropoff_datetime   Nullable(DateTime),
    passenger_count         Nullable(Float64),
    trip_distance           Nullable(Float64),
    RatecodeID              Nullable(Float64),
    store_and_fwd_flag      Nullable(String),
    PULocationID            Nullable(Int32),
    DOLocationID            Nullable(Int32),
    payment_type            Nullable(Int64),
    fare_amount             Nullable(Float64),
    extra                   Nullable(Float64),
    mta_tax                 Nullable(Float64),
    tip_amount              Nullable(Float64),
    tolls_amount            Nullable(Float64),
    improvement_surcharge   Nullable(Float64),
    total_amount            Nullable(Float64),
    congestion_surcharge    Nullable(Float64),
    Airport_fee             Nullable(Float64)
) ENGINE = MergeTree()
ORDER BY ingested_at;

-- ── SILVER LAYER ──────────────────────────────────────────────────────

-- Silver: Cleaned weather data
CREATE TABLE IF NOT EXISTS medallion.silver_weather (
    city            String,
    country         String,
    temperature     Float64,
    feels_like      Float64,
    humidity        Int32,
    wind_speed      Float64,
    weather_main    String,
    weather_desc    String,
    timestamp       DateTime,
    ingested_at     DateTime
) ENGINE = MergeTree()
ORDER BY (city, timestamp);

-- Silver: Cleaned NYC Taxi data
CREATE TABLE IF NOT EXISTS medallion.silver_nyc_taxi (
    VendorID                Int32,
    pickup_datetime         DateTime,
    dropoff_datetime        DateTime,
    passenger_count         Int32,
    trip_distance           Float64,
    PULocationID            Int32,
    DOLocationID            Int32,
    payment_type            Int32,
    fare_amount             Float64,
    tip_amount              Float64,
    total_amount            Float64,
    trip_duration_minutes   Float64
) ENGINE = MergeTree()
ORDER BY pickup_datetime;

-- ── GOLD LAYER ────────────────────────────────────────────────────────

-- Gold: Average weather per city per day
CREATE TABLE IF NOT EXISTS medallion.gold_weather_daily (
    city                String,
    country             String,
    date                Date,
    avg_temperature     Float64,
    avg_humidity        Float64,
    avg_wind_speed      Float64,
    dominant_weather    String
) ENGINE = MergeTree()
ORDER BY (city, date);

-- Gold: NYC Taxi KPIs per day
CREATE TABLE IF NOT EXISTS medallion.gold_nyc_taxi_daily (
    date                Date,
    total_trips         Int64,
    avg_fare            Float64,
    avg_trip_distance   Float64,
    avg_duration_mins   Float64,
    avg_passengers      Float64,
    total_revenue       Float64
) ENGINE = MergeTree()
ORDER BY date;