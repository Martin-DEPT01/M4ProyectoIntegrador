import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, year, month, dayofmonth, lit, when, round, pow

def main():
    spark = SparkSession.builder \
        .appName("ETL_Weather_Normalization") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.InstanceProfileCredentialsProvider") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .getOrCreate()
        # .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \

    spark.sparkContext.setLogLevel("WARN")


    # Parámetros calculo de POTENCIAL SOLAR
    EFICIENCIA_PANEL = 0.18    # 18% de eficiencia nominal del panel
    PERFORMANCE_RATIO = 0.75   # Pérdidas por cables, suciedad e inversor
    TEMP_STC = 25.0            # Temperatura estándar de prueba
    COEF_TEMP = 0.004          # Pierde 0.4% por cada grado sobre 25°C
    
    # Parámetros calculo de POTENCIAL EOLICO
    COEF_POTENCIA_WIND = 0.40  # Eficiencia aerogenerador (40%)
    R_AIRE = 287.05            # Constante gas ideal aire seco
    CONV_KELVIN = 273.15       # Constante para convertir C a K
    
    try:
        print(">>> Iniciando extracción de datos de S3 silver...")
        df_weather_unified = spark.read.parquet("s3a://m4-pi-medallion/silver/weather_unified_parquet/")

        # agregamos la potencia solar instantanea por cada medicion horaria
        print(">>> Ejecutando transformaciones y agregaciones...")
        df_result = (df_weather_unified
            .withColumn("potencia_solar_w", 
                (col("uvi") * lit(100)) * 
                lit(EFICIENCIA_PANEL) * 
                (lit(1) - when(col("temp") > TEMP_STC, 
                              (col("temp") - TEMP_STC) * lit(COEF_TEMP)).otherwise(0)) * 
                lit(PERFORMANCE_RATIO)
            )
            .withColumn("potencial_eolico_w", 
                # 1. Calculamos RHO (Densidad): Presión(Pa) / (R * Temp(K))
                ((col("pressure") * lit(100)) / (lit(R_AIRE) * (col("temp") + lit(CONV_KELVIN)))) * 
                # 2. Fórmula cinética: 0.5 * rho * v^3 * Eficiencia / 1000
                lit(0.5) * 
                pow(col("wind_speed"), 3) * 
                lit(COEF_POTENCIA_WIND)
            )
            .withColumn("potencial_renovable_total_w", col("potencia_solar_w") + col("potencial_eolico_w"))
        )

        total_rows = df_result.count()
        print(f">>> Registros procesados: {total_rows}")

        output_path = "s3a://m4-pi-medallion/gold/metrics/"
        print(f">>> Escribiendo a {output_path}")
        
        df_result.write.mode("overwrite").partitionBy("region", "year", "month").parquet(output_path)
        print(">>> ETL Finalizado exitosamente.")
    except Exception as e:
        print(f"ERROR: {str(e)}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()