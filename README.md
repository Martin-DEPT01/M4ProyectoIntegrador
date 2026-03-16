# Data Pipeline E2E: Weather Analitcs 🚀

Este proyecto implementa una arquitectura de datos moderna (**Modern Data Stack**) para la ingesta, procesamiento y modelado de datos via batch y streaming asociado al clima de Patagonia y Riohacha. La solución automatiza el flujo completo desde fuentes externas (APIs y archivos masivos) hasta un modelo de datos Data Lake sobre S3 con una arquitectura medallon.

## 🏗️ Arquitectura del Sistema

El ecosistema está desplegado sobre infraestructura de **AWS**, orquestado mediante **Apache Airflow** corriendo en contenedores **Docker** con transformaciones en **Apache Spark**. Para la parte de streaming se utilizaron conectores de **Airbyte** y scripts de **Python** para la carga inicial de los datos historicos.

1.  **Ingesta (Extract):**
    - **Pipeline Diario:** Consumo de API de clima (Openweather) con **Airbyte** -> Almacenamiento en **AWS S3** (JSON comprimido con GZ).
    - **Historico:** Ingesta de datasets masivos de metricas historicas del clima (2024) de Patagonia y Riohacha -> Almacenamiento en **AWS S3** (formato JSON).
2.  **Carga (Load):**
    - Scripts de Spark extraen los archivos de S3 en batch para validar, consolidar con joins y desplegar en las capas silver y gold. Finalmente se consumen los datos desde **Amazon Athena**.
3.  **Transformación (Transform - Spark):**
    - **Staging:** Limpieza, tipado y estandarización de datos.
    - **Core (Data Lake):** Uso de arquitectura medallon para separar las capas de procesado y de refinamiento de datos.

---

## 🛠️ Stack Tecnológico

- **Orquestación:** Apache Airflow 2.10.4 (Docker Compose).
- **Transformación:** Apache Spark.
- **Lenguajes:** Python.
- **Infraestructura Cloud (AWS):**
  - **EC2:**:
    - Instancia Ubuntu (8GB RAM) para el despliegue del orquestador Airflow.
    - Instancia Ubuntu (4GB RAM) para el despliegue de Spark.
  - **S3:** Data Lake para el almacenamiento de archivos en contexto Big Data.
  - **Airbyte:** Plataforma de servicios de conexion.
  - **IAM & Security Groups:** Gestión estricta de permisos y conectividad entre servicios.

---

## 📂 Estructura del Repositorio

```text
├── .github/                   		# AVANCE4: Logica de CI/CD
│   └── workflows/
│       └── spark_kafka_infra.yml	# Healthcheck de infraestructura Spark-Kafka
├── airflow/                   		# AVANCE4: Logica de orquestacion del pipeline
│   └── spark_silver_etl.py    		# Definición de DAGs
├── aws_athena/                   # Scripts de consultas en AWS Athena sobre la capa gold
│   ├── analysis_weather_business_queries.sql
│   └── create_table_gold_weather_metrics.sql
├── data/
│   ├── Patagonia_-41.json         	# Datos historicos de Patagonia
│   └── Riohacha_11_538415.json    	# Datos historicos de Riohacha
├── docs/
│   ├── Documento Tecnico.docx     	# AVANCE1: especificaciones y decisiones tecnologicas
│   ├── Preguntas de Negocio.txt
│   └── Reporte Preguntas de Negocio.ipynb    #Prints de ejecucion y respuestas a las preguntas de negocio
├── scripts/
│   ├── csv_raw.py 			   		    # Carga de datos historicos en S3
│   └── s3_connection.py       		# Configuracion de conexion al bucket raw
├── spark/                     		# AVANCE3: Lógica de Transformación y Validacion
│   ├── etl_to_gold.py           	# Transformaciones y Agregaciones de negocio para carga de la capa gold
│   └── etl_to_silver.py         	# Validacion, unificacion historico + stream (raw) para carga de la capa Silver
├── requirements.txt
├── .gitignore
└── README.md
```

## 👤 Autor

**Martin Daniel Tedesco** - _Data Engineer_
