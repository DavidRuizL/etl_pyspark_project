# Proyecto de ETL con Python y Spark

## Descripción 
Este proyecto es una implementación de un proceso ETL (Extract, Transform, Load) usando Python y Apache Spark. El objetivo es extraer datos de diversas fuentes (CSV, Parquet), transformarlos según las necesidades del negocio y cargarlos en una ubicación de salida.

## Tecnologías 
- **Python**: Lenguaje principal para el desarrollo de la lógica ETL.
- **Apache Spark**: Usado para procesar grandes volúmenes de datos de forma distribuida.
- **Pandas**: Utilizado para algunas operaciones previas de transformación de datos.
- **Openpyxl**: Para trabajar con archivos Excel (si fuera necesario en el futuro).

## Requisitos 
- **Python 3.7 o superior**.
- **Apache Spark 3.x**.
- Librerías requeridas:
  - `pandas`
  - `pyspark`
  - `openpyxl` (si se requiere lectura de Excel)




Para instalar las dependencias necesarias, ejecuta el siguiente comando en tu entorno de desarrollo:

```bash
pip install -r requirements.txt
```


# Estructura del Proyecto
```
etl_pyspark_project/
└── etl_project/
    ├── __init__.py                # Archivo de inicialización del paquete principal
    ├── config/
    │   ├── __init__.py            # Para convertir 'config' en un módulo
    │   └── config.py              # Configuración de rutas y variables globales
    ├── extract/
    │   ├── __init__.py            # Para convertir 'extract' en un módulo
    │   └── extract_data.py        # Código de extracción de datos
    ├── load/
    │   ├── __init__.py            # Para convertir 'load' en un módulo
    │   └── load_data.py           # Código de carga de datos
    ├── transform/
    │   ├── __init__.py            # Para convertir 'transform' en un módulo
    │   └── transform_data.py      # Código de transformación de datos
    ├── utils/
    │   ├── __init__.py            # Para convertir 'utils' en un módulo
    │   ├── spark_session.py       # Código para crear la sesión de Spark
    │── observability/
    │   ├── __init__.py        # Para inicializar el módulo de observabilidad
    │   ├── logging_config.py  # Configuración de logs para registrar eventos
    │   └── metrics.py         # Códigos para métricas de rendimiento y otros KPIs
    └── main.py                    # Archivo principal para ejecutar el ETL
```
# Configuración
El comportamiento de la aplicación está configurado a través del archivo config/config.py. Las siguientes variables son configurables:
```
INPUT_PATH = "data/input/"        # Ruta de entrada donde se encuentran los archivos de datos
OUTPUT_PATH = "data/output/datos" # Ruta de salida donde se guardarán los datos procesados
DATA_FORMAT = "csv"               # Formato de entrada de los datos. Puede ser "csv", "parquet", o "xlsx".
```
## Funciones principales
extract_data
Descripción: Extrae los datos de la fuente especificada (CSV, Parquet) y los devuelve como un lista de DataFrames de Spark.

transform_data
Descripción: Aplica las transformaciones necesarias a los datos extraídos. Estas transformaciones pueden incluir limpieza, agregación o combinación de datos.

load_data
Descripción: Carga los datos transformados en el destino especificado (por ejemplo, directorio de salida). Utiliza el formato CSV o Parquet dependiendo de la configuración.

## Escalabilidad y Observabilidad
Este proyecto está diseñado para ser escalable y manejar grandes volúmenes de datos utilizando Apache Spark. 
La escalabilidad se logra mediante el uso modulos independientes Extract, Transform y Load, ademas del procesamiento distribuido, lo que permite ejecutar las transformaciones en un cluster de Spark si es necesario.

Para la observabilidad, puedes agregar herramientas como loguru para registrar logs detallados durante el proceso de ETL. Los logs permiten monitorizar el estado del proceso y depurar cualquier problema en tiempo de ejecución.
