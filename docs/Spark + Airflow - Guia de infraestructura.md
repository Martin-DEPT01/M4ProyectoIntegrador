# Spark + Airflow: Infraestructura paso a paso:

<hr style="height: 2px; background-color: gray; border: none;">

## _FASE 0: Preparación del Entorno Local (Windows PowerShell)_

Autenticación Inicial en AWS CLI.
Debes tener tus credenciales programáticas (Access Key y Secret Key) de un usuario administrador.

1. **Configurar credenciales**

_PowerShell:_

```
aws configure

# Te pedirá:

# AWS Access Key ID: [Pega tu Key]

# AWS Secret Access Key: [Pega tu Secret]

# Default region name: [Ej. us-east-1]

# Default output format: json
```

2. **Generar tu llave SSH y guardarla con codificación compatible**

```
aws ec2 create-key-pair --key-name "SparkKeyV3" --query "KeyMaterial" --output text | Out-File -Encoding ascii -FilePath .\SparkKeyV3.pem
```

3. **Blindar la llave en Windows (evita el error "UNPROTECTED PRIVATE KEY FILE")**

```
icacls .\SparkKeyV2.pem /inheritance:r /grant:r "$($env:USERNAME):(R)"
```

4. **Creación del Rol IAM y Perfil para EC2 (Seguridad S3)**

   Evitamos errores de sintaxis en Windows creando un archivo temporal para la política de confianza.

_PowerShell:_

```
# Generamos el trust.json (política de confianza)

@'
{
"Version": "2012-10-17",
"Statement": [
{
"Effect": "Allow",
"Principal": {
"Service": "ec2.amazonaws.com"
},
"Action": "sts:AssumeRole"
}
]
}
'@ | Out-File -FilePath trust.json -Encoding ASCII
```

5. **Crear el Rol y asignarle permisos completos de S3**

```
aws iam create-role --role-name SparkS3Role --assume-role-policy-document file://trust.json
aws iam attach-role-policy --role-name SparkS3Role --policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess
```

6. **Crear el Perfil de Instancia y atar el Rol**

```
aws iam create-instance-profile --instance-profile-name SparkS3Profile
aws iam add-role-to-instance-profile --instance-profile-name SparkS3Profile --role-name SparkS3Role
```

7. **Limpiar archivo temporal**

```
Remove-Item trust.json
```

<hr style="height: 2px; background-color: gray; border: none;">

## _FASE 1: Lanzamiento de Servidores (Spark y Airflow)_

Ambas máquinas se lanzarán con 20GB de disco duro para evitar el error DiskErrorException, y del tipo m7i-flex.large (free tier).
Asegúrate de usar tu Security Group real en lugar de sg-0dcb....

PowerShell --->>>(VERIFICAR COMO ES CON m7i-flex.large Y Ubuntu)<<<---

1. **Levantamos las instancias EC2**

   Ejecuta todo este bloque junto:

_PowerShell_

```
# -- SERVIDOR 1: SPARK COMPUTE --

$SPARK_EC2 = aws ec2 run-instances `    --image-id ami-0c101f26f147fa7fd`
--count 1 `    --instance-type m7i-flex.large`
--key-name "SparkKeyV3" `    --security-group-ids sg-0dcb110700fba2b73`
--iam-instance-profile Name="SparkS3Profile" `    --metadata-options "HttpTokens=required,HttpPutResponseHopLimit=2,HttpEndpoint=enabled"`
--block-device-mappings 'DeviceName=/dev/xvda,Ebs={VolumeSize=20,VolumeType=gp3}' `    --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=SparkComputeNode}]'`
--query "Instances[0].InstanceId" --output text

# -- SERVIDOR 2: AIRFLOW ORCHESTRATOR --

$AIRFLOW_EC2 = aws ec2 run-instances `    --image-id ami-0c101f26f147fa7fd`
--count 1 `    --instance-type m7i-flex.large`
--key-name "SparkKeyV3" `    --security-group-ids sg-0dcb110700fba2b73`
--block-device-mappings 'DeviceName=/dev/xvda,Ebs={VolumeSize=16,VolumeType=gp3}' `    --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=AirflowNode}]'`
--query "Instances[0].InstanceId" --output text

Write-Host "Levantando infraestructura. Esperando 20 segundos..."
Start-Sleep -Seconds 20
```

2. **Obtenemos los IPs**

_PowerShell_

```
aws ec2 describe-instances --filters "Name=instance-state-name,Values=running" "Name=tag:Name,Values=SparkComputeNode,AirflowNode" --query "Reservations[*].Instances[*].{Nombre:Tags[?Key=='Name']|[0].Value, IP_Publica:PublicIpAddress, IP_Privada:PrivateIpAddress}" --output table
```

(⚠️ Anota de esa tabla: La IP Pública y Privada de SPARK, y la IP Pública de AIRFLOW).

<hr style="height: 2px; background-color: gray; border: none;">

## _FASE 2: Despliegue del Nodo de Spark (SSH a Servidor 1)_

1. Abre otra pestaña en tu terminal y conéctate al servidor de Spark usando su IP Pública:

```
ssh -i .\SparkKeyV3.pem ec2-user@<IP_PUBLICA_SPARK>
```

2. **Instalar Docker y Spark**

   Ejecuta todo este bloque junto:

Bash --->>>(ADAPTAR A Ubuntu y agregar el volumen)<<<---

_Bash_

```
sudo yum update -y && sudo yum install -y docker && sudo systemctl start docker && sudo systemctl enable docker && sudo usermod -aG docker ec2-user
newgrp docker
docker network create spark-net
docker run -d --net spark-net -p 8080:8080 -p 7077:7077 --name spark-master spark:4.0.1 /opt/spark/bin/spark-class org.apache.spark.deploy.master.Master
docker run -d --net spark-net --name spark-worker -e SPARK_WORKER_CORES=2 -e SPARK_WORKER_MEMORY=2g spark:4.0.1 /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker spark://spark-master:7077
```

3. **Crear e inyectar el script ETL**

- Transformacion de Bronze a Silver:

_Bash_

```
cat << 'EOF' > etl_to_silver.py
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
        df_final = df_hist_norm.union(df_stream_norm).withColumn("timestamp", from_unixtime(col("dt"))) \
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
EOF
```

- Transformacion de Silver a Gold:

_Bash_

```
cat << 'EOF' > etl_to_gold.py
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, year, month, dayofmonth, lit, when, round, pow

def main():
spark = SparkSession.builder \
 .appName("ETL_Weather_Normalization") \
 .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.InstanceProfileCredentialsProvider") \
 .config("spark.sql.parquet.compression.codec", "snappy") \
 .getOrCreate() # .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \

    spark.sparkContext.setLogLevel("WARN")


    # Parámetros calculo de POTENCIAL SOLAR
    EFICIENCIA_PANEL = 0.18    # 18% de eficiencia nominal del panel
    PERFORMANCE_RATIO = 0.75   # Pérdidas por cables, suciedad e inversor
    TEMP_STC = 25.0            # Temperatura estándar de prueba
    COEF_TEMP = 0.004          # Pierde 0.4% por cada grado sobre 25°C

    # Parámetros calculo de POTENCIAL EOLICO
    COEF_POTENCIA_WIND = 0.40  # Eficiencia aerogenerador (40%)
    R_AIRE = 287.05            # Constante gas ideal aire seco
    AREA_VIENTO = 1.0          # Por metro cuadrado de aspa
    CONV_KELVIN = 273.15       # Constante para convertir C a K

    try:
        print(">>> Iniciando extracción de datos de S3 silver...")
        df_weather_unified = spark.read.parquet("s3a://m4-pi-medallion/silver/weather_unified_parquet/")

        # agregamos la potencia solar instantanea por cada medicion horaria
        print(">>> Ejecutando transformaciones y agregaciones...")
        df_result = (df_weather_unified
            .withColumn("potencia_solar_w",
                (col("uvi") * lit(100)) * lit(EFICIENCIA_PANEL) *
                (lit(1) - when(col("temp") > TEMP_STC, (col("temp") - TEMP_STC) * lit(COEF_TEMP)).otherwise(0)) *
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

if **name** == "**main**":
main()
EOF

```

(Puedes salir de esta terminal, Spark ya está listo). --->>>(Esto no deberia hacer falta por ser bind mount)<<<---

<hr style="height: 2px; background-color: gray; border: none;">

## _FASE 3: Despliegue del Nodo Airflow (SSH a Servidor 2)_

1. **Conéctate al servidor de Airflow usando su IP Pública:**

```

ssh -i .\SparkKeyV3.pem ec2-user@<IP_PUBLICA_AIRFLOW>

```

2. **Instalar Docker y Airflow**

   Ejecuta todo este bloque:

Bash --->>>(ADAPTAR A Ubuntu)<<<---
_Bash_

```

sudo yum update -y && sudo yum install -y docker && sudo systemctl start docker && sudo systemctl enable docker && sudo usermod -aG docker ec2-user
newgrp docker
mkdir -p ~/airflow_dags
docker run -d --name airflow -p 8080:8080 -v /home/ec2-user/airflow_dags:/opt/airflow/dags apache/airflow:2.8.1 bash -c "airflow db migrate && airflow users create --username admin --password admin --firstname Data --lastname Engineer --role Admin --email admin@airflow.local && airflow standalone"

```

3. **Crear el DAG de Orquestación**

   (Nota: Reemplaza <IP_PRIVADA_SPARK> en el código por la IP que anotaste en la Fase 1, ej: 172.31.X.X):

_Bash_

```

cat << 'EOF' > ~/airflow_dags/spark_remote_etl.py
from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from datetime import datetime, timedelta

default_args = {'owner': 'data_engineer', 'depends_on_past': False, 'retries': 0}

with DAG('orquestador_remoto_spark', default_args=default_args, schedule_interval='@daily', start_date=datetime(2024, 1, 1), catchup=False) as dag:

    # 1. Tarea: Bronce a Plata (Normalización y Unión)
    spark_silver = SSHOperator(
        task_id='spark_bronce_to_silver',
        ssh_conn_id='spark_ec2_ssh',
        cmd_timeout=600,
        command="""
        docker exec spark-master /opt/spark/bin/spark-submit \
            --master spark://spark-master:7077 \
            --deploy-mode client \
            --conf spark.jars.ivy=/tmp/.ivy2 \
            --packages org.apache.hadoop:hadoop-aws:3.4.0,com.amazonaws:aws-java-sdk-bundle:1.12.367 \
            --executor-memory 1g \
            --executor-cores 2 \
            /opt/spark/scripts/etl_to_silver.py
        """
    )

    # 2. Tarea: Plata a Oro (Cálculo de Potenciales Solar/Eólico)
    spark_gold = SSHOperator(
        task_id='spark_silver_to_gold',
        ssh_conn_id='spark_ec2_ssh',
        cmd_timeout=600,
        command="""
        docker exec spark-master /opt/spark/bin/spark-submit \
            --master spark://spark-master:7077 \
            --deploy-mode client \
            --conf spark.jars.ivy=/tmp/.ivy2 \
            --packages org.apache.hadoop:hadoop-aws:3.4.0,com.amazonaws:aws-java-sdk-bundle:1.12.367 \
            --executor-memory 1g \
            --executor-cores 2 \
            /opt/spark/scripts/etl_to_gold.py
        """
    )

    # ETL flujo lineal
    spark_silver >> spark_gold

## EOF

```

<hr style="height: 2px; background-color: gray; border: none;">

## _FASE 4: La Conexión JSON (El Truco para la UI)_

Para evitar el error de formato al pegar tu llave en Airflow, ejecuta este comando en tu PowerShell local (Windows). Leerá tu archivo .pem, reemplazará los saltos de línea correctamente y te entregará el JSON exacto para copiar y pegar:

_PowerShell_

```

$clave = Get-Content .\SparkKeyV3.pem -Raw
$clave_formateada = $clave -replace "`r`n", "\n" -replace "`n", "\n"
$json_final = "{ `"private_key`": `"$clave_formateada`", `"no_host_key_check`": true }"
Write-Output $json_final | Set-Clipboard
Write-Host "¡JSON copiado a tu portapapeles! Listo para pegar en Airflow." -ForegroundColor Green

```

<hr style="height: 2px; background-color: gray; border: none;">

## _FASE 5: Orquestación Final_

1. Abre tu navegador web y ve a `http://<IP_PUBLICA_AIRFLOW>:8080`

2. Inicia sesión (admin / admin).

3. Ve a Admin -> Connections y haz clic en (+) Add a new record.

4. Llena los datos exactamente así:
   - **Connection Id:** _spark_ec2_ssh_

   - **Connection Type:** _SSH_

   - **Host:** _[La IP PRIVADA de tu servidor Spark]_

   - **Username:** _ec2-user_

   - **Extra:** _Presiona Ctrl+V (El JSON perfecto se generó en tu portapapeles en la Fase 4)._

5. Haz clic en Save.

6. Ve a la pantalla principal (DAGs).

7. Activa el DAG orquestador_remoto_spark con el switch a la izquierda.

8. Presiona Play (▶) -> Trigger DAG.

9. Entra al DAG, haz clic en el recuadro verde y ve a la pestaña Logs.

```

```
