import pandas as pd
import re
import numpy as np
from pyspark.sql.functions import col, when, regexp_replace
from pyspark.sql.types import DoubleType
from pyspark.sql import DataFrame

def eliminar_espacios_columnas(df):
    # Eliminar espacios en los nombres de las columnas
    df = df.toDF(*[col.replace(" ", "") for col in df.columns])
    return df


def limpiar_columnas_principalmente_numericas(df, threshold=0.8):
    """
    Identifica y limpia columnas que son principalmente numéricas, considerando espacios en blanco.

    Parameters:
    - df: DataFrame en PySpark.
    - threshold: Proporción mínima de valores numéricos en la columna para considerarla "principalmente numérica".

    Returns:
    - DataFrame con las columnas numéricas limpiadas.
    - Lista de columnas que fueron transformadas.
    """
    columnas_numericas_limpias = []

    for columna in df.columns:
        # Contamos los valores que contienen solo números, comas, puntos o espacios
        valid_numeric_count = df.filter(col(columna).rlike(r'^[0-9.,\s]*$')).count()
        total_count = df.filter(col(columna).isNotNull()).count()

        # Verifica si la columna es "principalmente numérica" usando el umbral especificado
        if total_count > 0 and (valid_numeric_count / total_count) >= threshold:
            # Agrega a la lista de columnas principalmente numéricas
            columnas_numericas_limpias.append(columna)

            # Limpia valores dejando solo números, comas y puntos (removemos otros caracteres)
            df = df.withColumn(columna, regexp_replace(col(columna), r'[^0-9.,]', ''))

            # Convierte la columna a numérica, asignando NaN a los valores no convertibles
            df = df.withColumn(columna, col(columna).cast(DoubleType()))

    return df, columnas_numericas_limpias


def limpiar_valores_invalidos(df: DataFrame):
    """
    Reemplaza los valores 'NULL', 'N/A' y espacios vacíos en el DataFrame con `null`.

    Parameters:
    - df: DataFrame en PySpark.

    Returns:
    - DataFrame con los valores inválidos reemplazados por `null`.
    """
    # Reemplaza los valores 'NULL', 'N/A' y espacios vacíos con null
    df = df.replace(['NULL', 'N/A'], None)  # Este reemplazo cubre los valores 'NULL' y 'N/A'
    
    # También reemplaza los valores vacíos o espacios en blanco
    for columna in df.columns:
        df = df.withColumn(columna, regexp_replace(col(columna), r'^\s*(NULL|N/A)?\s*$', 'null'))
    
    return df

def transform_data(df):
    """Aplica transformaciones necesarias a los datos."""
    df_limpio = eliminar_espacios_columnas(df)
    df_limpio, columnas_identificadas = limpiar_columnas_principalmente_numericas(df_limpio)
    df=limpiar_valores_invalidos(df_limpio)
    #print(f"DataFrame limpio {i+1}:\n", df)
    print(f"Columnas principalmente numéricas que fueron transformadas en DataFrame:\n", columnas_identificadas)
    df = df.dropDuplicates().na.drop()
    return df