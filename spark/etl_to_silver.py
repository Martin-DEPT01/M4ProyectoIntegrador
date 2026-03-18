import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, year, month, dayofmonth, lit, when, round

def main():
    spark = SparkSession.builder \
        .appName("ETL_Weather_Normalization") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.InstanceProfileCredentialsProvider") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .getOrCreate()
        # .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \

    spark.sparkContext.setLogLevel("WARN")

    try:
        print(">>> Iniciando extracción de datos de S3 bronze...")
        df_hist = spark.read.option("multiline", "true").json("s3a://m4-pi-medallion/bronze/historicos/")
        df_stream = spark.read.json("s3a://m4-pi-medallion/bronze/streams/")

        df_hist_norm = df_hist.select(
            col("dt").cast("long"), 
            round(col("lat").cast("double"), 4).alias("lat"), 
            round(col("lon").cast("double"), 4).alias("lon"), 
            col("uv_index").cast("double").alias("uvi"), 
            col("main.temp").cast("double").alias("temp"), 
            col("main.pressure").cast("integer").alias("pressure"), 
            col("main.humidity").cast("integer").alias("humidity"), 
            col("wind.speed").cast("double").alias("wind_speed"))

        df_stream_norm = df_stream.select(
            col("_airbyte_data.current.dt").cast("long").alias("dt"), 
            col("_airbyte_data.lat").cast("double").alias("lat"), 
            col("_airbyte_data.lon").cast("double").alias("lon"), 
            col("_airbyte_data.current.uvi").cast("double").alias("uvi"), 
            col("_airbyte_data.current.temp").cast("double").alias("temp"),
            col("_airbyte_data.current.pressure").cast("integer").alias("pressure"),  
            col("_airbyte_data.current.humidity").cast("integer").alias("humidity"), 
            col("_airbyte_data.current.wind_speed").cast("double").alias("wind_speed"))


        print(">>> Ejecutando Unión de esquemas...")
        df_final = df_hist_norm.unionByName(df_stream_norm).withColumn("timestamp", from_unixtime(col("dt"))) \
            .withColumn("year", year(col("timestamp"))) \
            .withColumn("month", month(col("timestamp"))) \
            .withColumn("day", dayofmonth(col("timestamp"))) \
            .withColumn("region", when((col("lat") == -41.8101) & (col("lon") == -68.9063), lit("Patagonia")) \
            .when((col("lat") == 11.5384) & (col("lon") == -72.9168), lit("Riohacha")) \
            .otherwise(lit("No Region"))
            )
        
        total_rows = df_final.count()
        print(f">>> Registros unificados: {total_rows}")

        output_path = "s3a://m4-pi-medallion/silver/weather_unified_parquet/"
        print(f">>> Escribiendo a {output_path}")

        df_final.write.mode("overwrite").partitionBy("region", "year", "month").parquet(output_path)
        print(">>> ETL Finalizado exitosamente.")

    except Exception as e:
        print(f"ERROR: {str(e)}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()