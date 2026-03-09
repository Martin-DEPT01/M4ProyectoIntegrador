import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, DoubleType, LongType, IntegerType

def main():
    spark = SparkSession.builder \
        .appName("KafkaToS3_WeatherStream") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.InstanceProfileCredentialsProvider") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    
    # 1. Definir el esquema (Schema) de la porción que nos interesa del JSON de la API
    esquema_weather = StructType([
        StructField("lat", DoubleType()),
        StructField("lon", DoubleType()),
        StructField("current", StructType([
            StructField("dt", LongType()),
            StructField("temp", DoubleType()),
            StructField("humidity", IntegerType())
        ]))
    ])

    print(">>> Conectando a Kafka...")
    # 2. Leer flujo desde Kafka
    df_kafka = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "IP-PRIVADA-KAFKA:9092") \
        .option("subscribe", "weather_stream") \
        .option("startingOffsets", "earliest") \
        .load()

    # 3. Transformación: Kafka devuelve el value en binario. Lo pasamos a String y extraemos con el esquema
    df_json = df_kafka.selectExpr("CAST(value AS STRING) as json_string")
    
    df_parsed = df_json.select(from_json(col("json_string"), esquema_weather).alias("data"))

    # 4. Aplanar (Flatten) las columnas
    df_final = df_parsed.select(
        col("data.current.dt").alias("timestamp"),
        col("data.lat").alias("latitud"),
        col("data.lon").alias("longitud"),
        col("data.current.temp").alias("temperatura"),
        col("data.current.humidity").alias("humedad")
    )

    print(">>> Iniciando escritura en S3...")
    # 5. Escribir flujo en S3 (Parquet) de manera continua (Append)
    query = df_final.writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", "s3a://silver-data-engineer/streaming_weather_parquet/") \
        .option("checkpointLocation", "s3a://silver-data-engineer/checkpoints/weather_stream/") \
        .trigger(processingTime="1 minute") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()