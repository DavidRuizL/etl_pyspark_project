from pyspark.sql import DataFrame
from utils.spark_session import get_spark_session
from config.config import INPUT_PATH, DATA_FORMAT  # Para manejar configuraciones de entrada

def extract_data() -> DataFrame:
    """
    Extrae datos desde la fuente especificada en INPUT_PATH.
    
    El formato de datos se puede configurar (por ejemplo: CSV, Parquet).
    """
    # Crear sesión de Spark
    spark = get_spark_session()
    
    # Dependiendo del formato de entrada (CSV, Parquet, etc.)
    if DATA_FORMAT == "csv":
        df = spark.read.csv(INPUT_PATH, header=True, inferSchema=True)
    elif DATA_FORMAT == "parquet":
        df = spark.read.parquet(INPUT_PATH)
    else:
        raise ValueError(f"Unsupported data format: {DATA_FORMAT}")

    return df