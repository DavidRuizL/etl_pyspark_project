def transform_data(df):
    """Aplica transformaciones necesarias a los datos."""
    # Ejemplo de transformación: Eliminar filas duplicadas y valores nulos
    df = df.dropDuplicates().na.drop()
    return df