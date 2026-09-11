# 🚀 Python para Ingeniería de Datos — Laboratorio

Laboratorio práctico para el curso **Python para Ingeniería de Datos**.

Durante el curso construiremos progresivamente un pipeline de datos utilizando:

* Python
* Pandas
* PostgreSQL
* Northwind
* MinIO
* Apache Parquet
* Apache Spark
* PySpark
* Spark SQL
* JupyterLab
* Docker Compose

Todo el laboratorio se ejecutará **localmente utilizando Docker**, por lo que no será necesario contratar servicios en la nube.

---

# 🎯 Objetivo

El objetivo del laboratorio es construir un pipeline de Ingeniería de Datos capaz de:

1. Extraer información desde una base de datos transaccional.
2. Integrar fuentes CSV y JSON.
3. Procesar información utilizando Python y Pandas.
4. Almacenar datos en un Data Lake local.
5. Trabajar con formatos analíticos como Apache Parquet.
6. Procesar datasets utilizando Apache Spark.
7. Realizar consultas utilizando Spark SQL.
8. Generar datasets preparados para análisis.

El dataset principal será la base de datos de ejemplo **Northwind**.

---

# 🏗️ Arquitectura

La arquitectura está intencionalmente simplificada para un curso de 20 horas.

```text
                    Docker Compose
                          │
          ┌───────────────┼───────────────┐
          │               │               │
          ▼               ▼               ▼
    PostgreSQL           MinIO         JupyterLab
     Northwind         Data Lake            │
          │               ▲                 │
          │               │          Python / Pandas
          │               │          PySpark / Spark
          │               │          Spark SQL
          │               │                 │
          └───────────────┼─────────────────┘
                          │
                          ▼
                    ┌─────────────┐
                    │  Data Lake  │
                    │             │
                    │ RAW         │
                    │ PROCESSED   │
                    │ CURATED     │
                    └─────────────┘
```

Apache Spark se ejecutará inicialmente en **Local Mode** dentro del mismo contenedor de JupyterLab.

No utilizaremos un cluster Spark independiente durante las primeras etapas del curso.

Esto nos permitirá concentrarnos en los conceptos de Ingeniería de Datos antes de introducir la complejidad de una arquitectura distribuida.

---

# 🧰 Stack tecnológico

| Tecnología     | Propósito                   |
| -------------- | --------------------------- |
| Python         | Lenguaje principal          |
| Pandas         | Procesamiento local         |
| PostgreSQL     | Base de datos transaccional |
| Northwind      | Dataset principal           |
| SQLAlchemy     | Acceso a base de datos      |
| Psycopg        | Driver PostgreSQL           |
| PyArrow        | Procesamiento de Parquet    |
| MinIO          | Data Lake / Object Storage  |
| Boto3          | Acceso S3 desde Python      |
| Apache Spark   | Procesamiento de datos      |
| PySpark        | API Python de Spark         |
| Spark SQL      | Consultas analíticas        |
| JupyterLab     | Entorno interactivo         |
| Docker         | Contenedores                |
| Docker Compose | Infraestructura local       |

---

# 📊 Northwind

Northwind representa una empresa ficticia dedicada a la comercialización de productos.

Entre sus principales entidades se encuentran:

```text
Customers
Employees
Orders
Order Details
Products
Categories
Suppliers
Shippers
```

Su modelo relacional nos permitirá estudiar la transformación desde un sistema transaccional **OLTP** hacia datasets optimizados para análisis.

Por ejemplo:

```text
Customers
    │
    ▼
Orders
    │
    ▼
Order Details
    │
    ▼
Products
    │
    ▼
Categories
```

---

# 🗄️ Data Lake

Utilizaremos **MinIO** como Object Storage local compatible con la API de Amazon S3.

Nuestro Data Lake tendrá tres zonas:

```text
data-lake/

├── raw/
│   ├── customers/
│   ├── orders/
│   ├── order_details/
│   ├── products/
│   ├── categories/
│   └── employees/
│
├── processed/
│   ├── customers/
│   ├── orders/
│   └── sales/
│
└── curated/
    ├── sales_by_month/
    ├── sales_by_country/
    ├── sales_by_product/
    ├── sales_by_category/
    └── customer_sales/
```

---

# 🥉 RAW

La zona RAW contiene los datos extraídos desde las fuentes con la menor cantidad posible de modificaciones.

```text
PostgreSQL
     │
     ▼
   Python
     │
     ▼
    MinIO
     │
     ▼
    raw/
```

Aquí podremos almacenar datos en formatos como:

```text
CSV
JSON
```

La idea principal es preservar una representación de los datos originales.

---

# 🥈 PROCESSED

La zona PROCESSED contendrá información:

* limpia;
* validada;
* tipada;
* normalizada;
* sin duplicados;
* preparada para análisis.

Utilizaremos principalmente:

```text
Apache Parquet
```

Por ejemplo:

```text
processed/orders/

├── year=2025/
│   ├── month=01/
│   └── month=02/
│
└── year=2026/
    ├── month=01/
    └── month=02/
```

Esto permitirá introducir conceptos como:

* particionamiento;
* almacenamiento columnar;
* compresión;
* predicate pushdown;
* reducción de I/O.

---

# 🥇 CURATED / GOLD

Contiene datasets preparados para consumo analítico.

Ejemplos:

```text
sales_by_month
sales_by_country
sales_by_product
sales_by_category
customer_sales
employee_performance
```

---

# 🔄 Pipeline

El pipeline principal será:

```text
Northwind / PostgreSQL
          │
          ▼
       Extract
          │
       Python
          │
          ▼
     MinIO / RAW
          │
          ▼
     Apache Spark
          │
          ▼
   Clean / Transform
          │
          ▼
       Parquet
          │
          ▼
 MinIO / PROCESSED
          │
          ▼
      Spark SQL
          │
          ▼
  MinIO / CURATED
```

---

# 📈 Métricas

Una de las métricas principales será el ingreso generado por cada línea de una orden.

```text
Revenue = UnitPrice × Quantity × (1 - Discount)
```

A partir de los datos podremos obtener métricas como:

```text
Revenue mensual
Revenue anual
Revenue por país
Revenue por categoría
Revenue por producto
Revenue por cliente
Revenue por empleado
Ticket promedio
Top productos
Top clientes
```

---

# ⭐ Modelo analítico

A partir del modelo transaccional de Northwind construiremos un modelo analítico simplificado.

```text
                    dim_customer
                         │
                         ▼
dim_product ──────── fact_sales ──────── dim_employee
                         ▲
                         │
                      dim_date
```

Una posible estructura de `fact_sales` será:

```text
order_id
order_date
customer_id
product_id
employee_id
country
category
quantity
unit_price
discount
revenue
```

Esto permitirá introducir conceptos básicos de:

* tablas de hechos;
* dimensiones;
* esquema estrella;
* granularidad;
* métricas;
* claves de dimensión.

---

# 📁 Estructura del proyecto

```text
python-data-engineering-lab/

├── docker-compose.yml
├── .env.example
├── requirements.txt
├── README.md
│
├── data/
│   ├── csv/
│   └── json/
│
├── notebooks/
│   ├── 01-python/
│   ├── 02-pandas/
│   ├── 03-sql/
│   ├── 04-data-lake/
│   └── 05-spark/
│
├── src/
│   ├── extract/
│   ├── transform/
│   ├── load/
│   ├── quality/
│   └── utils/
│
├── sql/
│   ├── northwind/
│   └── analytics/
│
└── tests/
```

---

# 🐳 Infraestructura Docker

Para mantener el laboratorio sencillo utilizaremos solamente tres servicios principales:

```text
postgres
minio
jupyter
```

La arquitectura Docker será:

```text
┌─────────────────────────────────────────────┐
│                Docker Compose               │
│                                             │
│   ┌────────────┐       ┌────────────────┐  │
│   │ PostgreSQL │       │     MinIO      │  │
│   │            │       │                │  │
│   │ Northwind  │       │   Data Lake    │  │
│   └──────┬─────┘       └───────▲────────┘  │
│          │                     │            │
│          │                     │            │
│          ▼                     │            │
│   ┌──────────────────────────────────────┐  │
│   │             JupyterLab               │  │
│   │                                      │  │
│   │ Python                               │  │
│   │ Pandas                               │  │
│   │ PySpark                              │  │
│   │ Apache Spark                         │  │
│   │ Spark SQL                            │  │
│   └──────────────────────────────────────┘  │
│                                             │
└─────────────────────────────────────────────┘
```

---

# 🐳 docker-compose.yml

El laboratorio utiliza imágenes públicas disponibles en sus respectivos registros.

```yaml
services:

  postgres:
    image: postgres:17
    container_name: northwind-postgres

    environment:
      POSTGRES_DB: northwind
      POSTGRES_USER: northwind
      POSTGRES_PASSWORD: northwind

    ports:
      - "5432:5432"

    volumes:
      - postgres_data:/var/lib/postgresql/data


  minio:
    image: quay.io/minio/minio
    container_name: data-lake

    command: server /data --console-address ":9001"

    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin

    ports:
      - "9000:9000"
      - "9001:9001"

    volumes:
      - minio_data:/data


  jupyter:
    image: quay.io/jupyter/pyspark-notebook:latest
    container_name: data-engineering-lab

    ports:
      - "8888:8888"
      - "4040:4040"

    volumes:
      - ./notebooks:/home/jovyan/work/notebooks
      - ./src:/home/jovyan/work/src
      - ./data:/home/jovyan/work/data

    environment:
      JUPYTER_ENABLE_LAB: "yes"

    depends_on:
      - postgres
      - minio


volumes:

  postgres_data:

  minio_data:
```

---

# 📥 Verificar las imágenes

Antes de levantar por primera vez el laboratorio se pueden descargar explícitamente las imágenes:

```bash
docker pull postgres:17
docker pull quay.io/minio/minio
docker pull quay.io/jupyter/pyspark-notebook:latest
```

Si las tres operaciones terminan correctamente, las imágenes necesarias están disponibles localmente.

---

# 🚀 Levantar el laboratorio

## 1. Clonar el repositorio

```bash
git clone <repository-url>

cd python-data-engineering-lab
```

---

## 2. Crear los directorios

Si todavía no existen:

```bash
mkdir -p notebooks src data
```

---

## 3. Levantar los servicios

```bash
docker compose up -d
```

---

## 4. Verificar

```bash
docker compose ps
```

Deberán aparecer tres contenedores:

```text
northwind-postgres
data-lake
data-engineering-lab
```

---

# 🌐 Servicios

| Servicio      | Dirección               |
| ------------- | ----------------------- |
| JupyterLab    | `http://localhost:8888` |
| MinIO Console | `http://localhost:9001` |
| MinIO S3 API  | `http://localhost:9000` |
| Spark UI      | `http://localhost:4040` |
| PostgreSQL    | `localhost:5432`        |

> Spark UI estará disponible cuando exista un SparkContext activo.

---

# 🔑 Obtener el token de Jupyter

Si Jupyter solicita un token:

```bash
docker logs data-engineering-lab
```

También puede utilizarse:

```bash
docker exec data-engineering-lab jupyter server list
```

El resultado mostrará una dirección similar a:

```text
http://localhost:8888/lab?token=...
```

---

# ⚡ Apache Spark

Apache Spark se ejecutará inicialmente en **Local Mode**.

Desde un Notebook:

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("NorthwindDataEngineering")
    .master("local[*]")
    .getOrCreate()
)
```

`local[*]` indica a Spark que ejecute el procesamiento localmente utilizando los cores disponibles.

Podemos comprobarlo:

```python
spark.sparkContext.master
```

Y consultar la versión:

```python
spark.version
```

---

# 🔥 Spark UI

Una vez iniciado un SparkContext, podremos acceder a:

```text
http://localhost:4040
```

Spark UI permitirá observar conceptos fundamentales como:

```text
Jobs
Stages
Tasks
Executors
Storage
SQL
```

Esto será especialmente útil para entender cómo Spark ejecuta internamente nuestras transformaciones.

---

# 🐍 Python

La imagen de PySpark Notebook incluye Python, JupyterLab, Spark y varias librerías científicas.

Durante el curso utilizaremos adicionalmente paquetes como:

```text
pandas
sqlalchemy
psycopg
requests
pyarrow
boto3
pyspark
```

Si posteriormente necesitamos dependencias adicionales, podremos crear una imagen propia basada en:

```dockerfile
FROM quay.io/jupyter/pyspark-notebook:latest
```

en lugar de utilizar nombres de imágenes locales inexistentes.

---

# 🔌 Comunicación entre contenedores

Una consideración importante es que los contenedores se comunican utilizando los nombres de los servicios de Docker Compose.

Desde nuestra computadora utilizamos:

```text
PostgreSQL → localhost:5432
MinIO      → localhost:9000
```

Pero desde Jupyter utilizaremos:

```text
PostgreSQL → postgres:5432
MinIO      → minio:9000
```

Por ejemplo, una conexión PostgreSQL desde Python dentro de Jupyter será conceptualmente:

```python
postgresql://northwind:northwind@postgres:5432/northwind
```

Y el endpoint S3 de MinIO será:

```text
http://minio:9000
```

---

# 🪣 Acceder a MinIO desde Python

Podremos utilizar Boto3:

```python
import boto3

s3 = boto3.client(
    "s3",
    endpoint_url="http://minio:9000",
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin",
    region_name="us-east-1",
)
```

Por ejemplo, crear el bucket del laboratorio:

```python
s3.create_bucket(
    Bucket="data-lake"
)
```

Nuestra estructura lógica será:

```text
data-lake

raw/
processed/
curated/
```

---

# 🧪 Calidad de datos

Un pipeline no solamente debe mover datos.

También debe garantizar que los datos sean confiables.

Con Pandas podremos realizar validaciones como:

```python
assert df["customer_id"].notnull().all()
assert (df["quantity"] > 0).all()
assert (df["unit_price"] >= 0).all()
```

Con Spark:

```python
assert df.filter(df.customer_id.isNull()).count() == 0

assert df.filter(df.quantity <= 0).count() == 0
```

---

# 📝 Logging

Los pipelines utilizarán el módulo estándar `logging`.

```python
import logging

logger = logging.getLogger(__name__)

logger.info("Starting sales extraction")

# procesamiento...

logger.info("Sales extraction completed")
```

El objetivo es evitar depender exclusivamente de:

```python
print()
```

para diagnosticar pipelines.

---

# 📚 Ruta de aprendizaje

El laboratorio evolucionará progresivamente:

```text
Python
   │
   ▼
Pandas
   │
   ▼
SQL / PostgreSQL
   │
   ▼
Northwind
   │
   ▼
CSV / JSON
   │
   ▼
Parquet
   │
   ▼
Object Storage
   │
   ▼
Data Lake / MinIO
   │
   ▼
Apache Spark
   │
   ▼
PySpark
   │
   ▼
Spark SQL
   │
   ▼
Modelo Analítico
```

La intención es que Spark aparezca después de comprender los problemas que intenta resolver.

---

# 🆚 Pandas vs Spark

Durante el curso realizaremos algunas transformaciones primero con Pandas y posteriormente con Spark.

Por ejemplo:

```text
Pandas                     Spark

DataFrame                  DataFrame

read_csv()                 spark.read.csv()

read_parquet()             spark.read.parquet()

merge()                    join()

groupby()                  groupBy()

query()                    filter()
```

Esto permitirá comprender las similitudes entre ambas herramientas y, sobre todo, sus diferencias de arquitectura y escalabilidad.

---

# 🏆 Proyecto final

Al finalizar el laboratorio, el estudiante deberá construir un pipeline que:

1. Extraiga información desde Northwind.
2. Integre al menos una fuente CSV o JSON.
3. Almacene los datos originales en RAW.
4. Limpie y valide los datos.
5. Transforme los datasets a Parquet.
6. Almacene los resultados en PROCESSED.
7. Procese información utilizando PySpark.
8. Construya datasets analíticos.
9. Almacene los resultados en CURATED.
10. Ejecute consultas utilizando Spark SQL.
11. Calcule métricas de negocio.

El resultado será un pequeño **Data Lake local construido con tecnologías open source**.

---

# 🧹 Detener el laboratorio

Para detener los contenedores:

```bash
docker compose down
```

Los datos persistirán en los volúmenes Docker.

Para eliminar también los volúmenes:

```bash
docker compose down -v
```

> ⚠️ Este último comando elimina los datos almacenados en PostgreSQL y MinIO.

---

# 🔍 Troubleshooting

## Ver estado

```bash
docker compose ps
```

## Ver todos los logs

```bash
docker compose logs
```

## Logs de PostgreSQL

```bash
docker compose logs postgres
```

## Logs de MinIO

```bash
docker compose logs minio
```

## Logs de Jupyter

```bash
docker compose logs jupyter
```

## Reiniciar

```bash
docker compose restart
```

## Empezar completamente desde cero

```bash
docker compose down -v
docker compose pull
docker compose up -d
```

---

# 🎓 Objetivo final

Al terminar el laboratorio, el estudiante no solamente deberá saber utilizar Pandas o PySpark.

Deberá ser capaz de explicar y construir el flujo:

```text
SOURCE
   │
   ▼
EXTRACT
   │
   ▼
RAW
   │
   ▼
CLEAN
   │
   ▼
TRANSFORM
   │
   ▼
PROCESSED
   │
   ▼
CURATED
   │
   ▼
ANALYTICS
```

Y, sobre todo, comprender **por qué existe cada etapa dentro de una arquitectura moderna de Ingeniería de Datos**.
