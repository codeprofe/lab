# 🚀 Python para Ingeniería de Datos — Laboratorio

Laboratorio práctico para el curso **Python para Ingeniería de Datos**.

Durante el curso construiremos progresivamente un pipeline de datos completo utilizando Python, PostgreSQL, Pandas, Apache Spark, MinIO y formatos analíticos como Parquet.

El objetivo es simular una arquitectura moderna de Ingeniería de Datos completamente **local utilizando Docker**, sin necesidad de contratar servicios en la nube.

---

## 🎯 Objetivo

Construir un pipeline de datos que permita:

1. Extraer información desde diferentes fuentes.
2. Procesar y limpiar datos utilizando Python y Pandas.
3. Almacenar información en un Data Lake.
4. Utilizar formatos optimizados como Parquet.
5. Procesar datasets utilizando Apache Spark.
6. Consultar información mediante Spark SQL.
7. Generar datasets analíticos preparados para consumo.

El dataset principal utilizado durante el laboratorio será **Northwind**.

---

# 🏗️ Arquitectura

```text
                         DATA SOURCES

                 ┌─────────────────────┐
                 │     PostgreSQL      │
                 │      Northwind      │
                 └──────────┬──────────┘
                            │
              ┌─────────────┼─────────────┐
              │             │             │
              │         CSV / JSON        │
              │             │             │
              ▼             ▼             ▼
         ┌────────────────────────────────────┐
         │              PYTHON                │
         │                                    │
         │ Pandas / SQLAlchemy / Requests     │
         │ PyArrow / Boto3                    │
         └─────────────────┬──────────────────┘
                           │
                           ▼
                 ┌───────────────────┐
                 │       MinIO       │
                 │                   │
                 │     DATA LAKE     │
                 │                   │
                 │       RAW         │
                 └─────────┬─────────┘
                           │
                           ▼
                  ┌─────────────────┐
                  │  Apache Spark   │
                  │                 │
                  │    PySpark      │
                  │   Spark SQL     │
                  └────────┬────────┘
                           │
                           ▼
                 ┌───────────────────┐
                 │       MinIO       │
                 │                   │
                 │    PROCESSED      │
                 │      Parquet      │
                 └─────────┬─────────┘
                           │
                           ▼
                    Transformaciones
                           │
                           ▼
                 ┌───────────────────┐
                 │       MinIO       │
                 │                   │
                 │      CURATED      │
                 │       / GOLD      │
                 └───────────────────┘
```

---

# 🧰 Tecnologías

| Tecnología | Uso |
|---|---|
| Python | Desarrollo de pipelines |
| Pandas | Procesamiento local |
| PostgreSQL | Base de datos transaccional |
| Northwind | Dataset principal |
| SQLAlchemy | Acceso a base de datos |
| PyArrow | Manejo de Parquet |
| MinIO | Data Lake / Object Storage |
| Boto3 | Acceso S3 desde Python |
| Apache Spark | Procesamiento distribuido |
| PySpark | Spark desde Python |
| Spark SQL | Consultas analíticas |
| JupyterLab | Ambiente interactivo |
| Docker | Contenedores |
| Docker Compose | Orquestación del laboratorio |

---

# 📊 Dataset Northwind

Northwind representa una empresa ficticia dedicada a la comercialización de productos.

Contiene información sobre:

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

Su modelo transaccional nos permitirá estudiar la transformación desde un sistema **OLTP** hacia estructuras optimizadas para análisis.

Ejemplo:

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

Utilizaremos **MinIO** como almacenamiento de objetos compatible con S3.

Nuestro Data Lake tendrá tres zonas principales:

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

## RAW

Contiene los datos originales extraídos desde las fuentes.

Los datos almacenados aquí deben sufrir la menor cantidad posible de modificaciones.

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

---

## PROCESSED

Contiene información:

- limpia;
- validada;
- tipada;
- normalizada;
- sin duplicados;
- preparada para procesamiento analítico.

El formato principal será:

```text
Apache Parquet
```

Ejemplo:

```text
processed/orders/

year=2025/
    month=01/
    month=02/

year=2026/
    month=01/
    month=02/
```

Esto nos permitirá estudiar **particionamiento de datos**.

---

## CURATED / GOLD

Contiene datasets preparados para análisis y consumo.

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

El pipeline completo del laboratorio será:

```text
Northwind PostgreSQL
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

Durante el laboratorio construiremos diferentes métricas de negocio.

Por ejemplo:

### Revenue

```text
Revenue = UnitPrice × Quantity × (1 - Discount)
```

A partir de esta métrica podremos calcular:

```text
Revenue mensual

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

A partir del modelo transaccional de Northwind construiremos un modelo simplificado para análisis.

```text
                    dim_customer
                         │
                         │
                         ▼
dim_product ──────── fact_sales ──────── dim_employee
                         ▲
                         │
                         │
                      dim_date
```

La tabla de hechos contendrá información similar a:

```text
fact_sales

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

---

# 📁 Estructura del proyecto

```text
python-data-engineering-lab/

├── docker-compose.yml
├── Dockerfile
├── requirements.txt
├── .env.example
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

# 🐳 Docker

Toda la infraestructura del laboratorio se ejecutará mediante Docker Compose.

Servicios principales:

```text
postgres
minio
spark-master
spark-worker
jupyter
```

Arquitectura:

```text
┌─────────────────────────────────────────┐
│                 Docker                  │
│                                         │
│  PostgreSQL        MinIO                │
│      │               ▲                  │
│      │               │                  │
│      ▼               │                  │
│   Jupyter ───────────┤                  │
│      │               │                  │
│      ▼               │                  │
│ Apache Spark ────────┘                  │
│                                         │
└─────────────────────────────────────────┘
```

---

# 🚀 Levantar el laboratorio

## 1. Clonar el repositorio

```bash
git clone <repository-url>

cd python-data-engineering-lab
```

## 2. Crear variables de entorno

```bash
cp .env.example .env
```

## 3. Levantar los servicios

```bash
docker compose up -d
```

## 4. Verificar los contenedores

```bash
docker compose ps
```

Deberán aparecer los servicios:

```text
postgres
minio
spark-master
spark-worker
jupyter
```

---

# 🌐 Servicios

Una vez levantado el laboratorio tendremos disponibles:

| Servicio | Dirección |
|---|---|
| JupyterLab | `http://localhost:8888` |
| MinIO Console | `http://localhost:9001` |
| MinIO S3 API | `http://localhost:9000` |
| Spark Master UI | `http://localhost:8080` |
| PostgreSQL | `localhost:5432` |

Las credenciales y configuraciones específicas estarán definidas en:

```text
.env
```

---

# 🐍 Librerías Python

Las principales dependencias serán:

```text
pandas
sqlalchemy
psycopg
requests
pyarrow
boto3
pyspark
```

---

# 🧪 Calidad de datos

Un pipeline no solamente debe mover información.

También debe garantizar que los datos sean confiables.

Durante el laboratorio realizaremos validaciones como:

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

Los pipelines utilizarán `logging` en lugar de depender exclusivamente de `print()`.

```python
import logging

logger = logging.getLogger(__name__)

logger.info("Starting sales extraction")

logger.info("Sales extraction completed")
```

Esto permitirá introducir buenas prácticas para pipelines que posteriormente pueden ejecutarse de manera automatizada.

---

# 📚 Ruta del laboratorio

El laboratorio se desarrollará progresivamente.

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
CSV / JSON
   │
   ▼
Parquet
   │
   ▼
Data Lake
   │
   ▼
MinIO
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

---

# 🏆 Proyecto final

Al finalizar el laboratorio, el estudiante deberá construir un pipeline que:

1. Extraiga datos desde Northwind.
2. Integre al menos una fuente externa CSV o JSON.
3. Almacene los datos originales en la zona RAW.
4. Limpie y valide los datos.
5. Convierta los datasets a Parquet.
6. Almacene los resultados en PROCESSED.
7. Procese la información utilizando PySpark.
8. Construya datasets analíticos en CURATED.
9. Ejecute consultas mediante Spark SQL.
10. Genere métricas de negocio a partir de los datos.

El resultado será un pequeño **Data Lake local construido completamente con tecnologías open source**.

---

## 🎓 Objetivo final

Al terminar el laboratorio, el estudiante no solamente deberá saber utilizar Pandas o PySpark.

Deberá ser capaz de explicar y construir el flujo:

```text
SOURCE
   ↓
EXTRACT
   ↓
RAW
   ↓
CLEAN
   ↓
TRANSFORM
   ↓
PROCESSED
   ↓
CURATED
   ↓
ANALYTICS
```

Y comprender **por qué existe cada etapa dentro de una arquitectura moderna de Ingeniería de Datos**.