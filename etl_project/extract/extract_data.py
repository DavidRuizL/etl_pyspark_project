import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from pyspark.sql import DataFrame
from utils.spark_session import get_spark_session
from config.config import INPUT_PATH, DATA_FORMAT  # Para manejar configuraciones de entrada

def extract_data() -> list:
    """
    Extrae datos desde la fuente especificada en INPUT_PATH.
    
    Si el archivo es un Excel (.xlsx), lee todas las hojas excepto la primera 
    y las guarda como archivos CSV. Luego, lee esos archivos CSV con Spark.
    Si el formato es CSV o Parquet, el comportamiento sigue siendo el mismo que en el código original.
    """
    # Crear sesión de Spark
    spark = get_spark_session()
  
    # Si el formato es CSV
    if DATA_FORMAT == "csv":
        # Obtener la lista de archivos CSV en la carpeta INPUT_PATH
        csv_files = [f for f in os.listdir(INPUT_PATH) if f.endswith('.csv')]
        
        dfs_spark = []  # Lista para almacenar los DataFrames de Spark

        # Leer cada archivo CSV y convertirlo a DataFrame de Spark
        for file in csv_files:
            file_path = os.path.join(INPUT_PATH, file)
            df = spark.read.csv(file_path, header=True, inferSchema=True)
            dfs_spark.append(df)  # Añadir DataFrame a la lista
        
        return dfs_spark  # Devolver la lista de DataFrames de Spark
    
    # Si el formato es Parquet
    elif DATA_FORMAT == "parquet":
        df = spark.read.parquet(INPUT_PATH)
        return [(df)]  # Devuelve como lista con un DataFrame
    
    # Si el formato no es soportado
    else:
        raise ValueError(f"Unsupported data format: {DATA_FORMAT}")

df=extract_data()

for idx, df in enumerate(df):
    print(f"Descripción del DataFrame {idx + 1}:")
    print(df)