CREATE EXTERNAL TABLE IF NOT EXISTS gold_weather_metrics (
  day INT,
  dt BIGINT,
  lat DOUBLE,
  lon DOUBLE,
  uvi DOUBLE,
  temp DOUBLE,
  pressure INT,
  humidity INT,
  wind_speed DOUBLE,
  potencia_solar_w DOUBLE,
  potencial_eolico_w DOUBLE,
  potencial_renovable_total_w DOUBLE
)
PARTITIONED BY (region STRING, year INT, month INT)
STORED AS PARQUET
LOCATION 's3://m4-pi-medallion/gold/metrics/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

MSCK REPAIR TABLE gold_weather_metrics;