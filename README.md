# Data Pipeline E2E: Weather Analitcs 🚀

Este proyecto implementa una arquitectura de datos moderna (**Modern Data Stack**) para la ingesta, procesamiento y modelado de datos via batch y streaming asociado al clima de Patagonia y Riohacha. La solución automatiza el flujo completo desde fuentes externas (APIs y archivos masivos) hasta un modelo de datos Data Lake sobre S3 con una arquitectura medallon.

## 🏗️ Arquitectura del Sistema

El ecosistema está desplegado sobre infraestructura de **AWS**, orquestado mediante **Apache Airflow** corriendo en contenedores **Docker** con transformaciones en **Apache Spark**. Para la parte de streaming se utilizaron conectores de **Airbyte** y scripts de **Python** para la carga inicial de los datos historicos.

1.  **Ingesta (Extract):**
    - **Pipeline Diario:** Consumo de API de clima (Openweather) con **Airbyte** por hora -> Almacenamiento en **AWS S3** (JSON comprimido con GZ).
    - **Historico:** Ingesta de datasets masivos de metricas historicas del clima (2024/2025) de Patagonia y Riohacha -> Almacenamiento en **AWS S3** (formato JSON).
2.  **Carga (Load):**
    - Scripts de Spark extraen los archivos de S3 en batch para validar, consolidar con joins, transformar, agregar y desplegar en las capas silver y gold. Finalmente se consumen los datos desde **Amazon Athena**.
3.  **Transformación (Transform - Spark):**
    - **Staging:** Limpieza, tipado y estandarización de datos.
    - **Core (Data Lake):** Uso de arquitectura medallon para separar las capas de procesado y de refinamiento de datos.

---

## 🛠️ Stack Tecnológico

- **Orquestación:** Apache Airflow (Docker Compose).
- **Transformación:** Apache Spark.
- **Lenguajes:** Python.
- **Infraestructura Cloud (AWS):**
  - **EC2:**:
    - Instancia Ubuntu (8GB RAM) para el despliegue del orquestador _Airflow_.
    - Instancia Ubuntu (8GB RAM) para el despliegue de _Spark_.
  - **S3:** Data Lake para el almacenamiento de archivos en contexto Big Data.
  - **Airbyte:** Plataforma de servicios de conexion.
  - **IAM & Security Groups:** Gestión estricta de permisos y conectividad entre servicios.
- **CI/CD:** GitHub Actions para validación automática de scripts de Spark, DAGs de Airflow y queries de Athena.

---

## 📈 Lógica de Negocio (Métricas Gold)

El cálculo del potencial se basa en las siguientes variables físicas:

1. **Potencial Solar:** Índice UV corregido por eficiencia de paneles (18%) y degradación por temperatura extrema (STC 25°C).

2. **Potencial Eólico:** Cálculo de la **Densidad del Aire** usando presión y temperatura, aplicado a la fórmula cinética de energía eólica (0.5 \* densidad \* velocidad_viento_al_cubo \* eficiencia_aerogenerador).

---

## 📂 Estructura del Repositorio

```text
├── .github/                   		# AVANCE4: Logica de CI/CD
│   └── workflows/
│       └── scripts_validation.yml	# Verificacion de good practice en scripts de Airflow, Spark y AWS Athena (sql)
├── airflow/                   		  # AVANCE4: Logica de orquestacion del pipeline
│   └── spark_silver_etl.py    		  # Definición de DAGs
├── aws_athena/                     # Scripts de consultas en AWS Athena sobre la capa gold
│   ├── analysis_weather_business_queries.sql
│   └── create_table_gold_weather_metrics.sql
├── data/
│   ├── Patagonia_-41.json         	# Datos historicos de Patagonia
│   └── Riohacha_11_538415.json    	# Datos historicos de Riohacha
├── docs/
│   ├── Documento Tecnico.docx     	# AVANCE1: especificaciones y decisiones tecnologicas
│   ├── Evidencias de ejecucion.ipynb   # Evidencias Airbyte + Airflow + GitHub Actions
│   ├── Preguntas de Negocio.txt
│   ├── Reporte Preguntas de Negocio.ipynb    #Prints de ejecucion y respuestas a las preguntas de negocio
│   └── Spark + Airflow - Guia de infraestructura.md    #Guia para montar toda la infraestructura AWS EC2
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

---

## 🛠️ Configuración del Entorno (Setup)

### Requisitos Previos

- Bucket S3 con estructura de carpetas `bronze/`, `silver/`, `gold/`.
- Credenciales de AWS configuradas en el clúster de Spark.
- Creacion de un entorno virtual:
  ```bash
  python3 -m venv venv
  ```
- Activacion del mismo:
  ```bash
  source venv/bin/activate
  ```
- Instalar las librerías de validación y procesamiento.

  ```bash
  # Asegura que pip esté actualizado
  pip install --upgrade pip

  # Instalación desde el archivo de requerimientos
  pip install -r requirements.txt
  ```

- Por ultimo para un despliegue desde cero, consultá nuestra guía paso a paso en:
  👉 **[Guía de Infraestructura y Despliegue](./docs/Spark%20+%20Airflow%20-%20Guia%20de%20infraestructura.md)**

---

## 💡 Hallazgos y Resultados (Business Insights)

Tras el análisis de los datos en la capa **Gold** mediante Amazon Athena, se desprenden las siguientes conclusiones estratégicas para la toma de decisiones energéticas:

- **Complementariedad Regional:** Mientras que **Riohacha** presenta un potencial solar constante durante todo el año (baja variabilidad estacional), la **Patagonia** compensa su menor radiación invernal con un potencial eólico significativamente superior y ráfagas de viento de alta energía.
- **Factor de Eficiencia:** Se identificó que en Riohacha, a pesar de tener mayor radiación, la eficiencia de los paneles disminuye un **~3-5%** en días de calor extremo (>32°C), validando la importancia del coeficiente de temperatura incluido en el ETL.
- **Estabilidad del Recurso:** La Patagonia presenta una intermitencia eólica más brusca, requiriendo sistemas de almacenamiento (baterías) más robustos en comparación con la infraestructura solar sugerida para el Caribe colombiano.

## 👤 Autor

**Martin Daniel Tedesco** - _Data Engineer_
