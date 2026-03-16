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