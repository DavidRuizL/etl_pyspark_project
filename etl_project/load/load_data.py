def load_data(df, destination_path):
    """Carga los datos transformados en el destino especificado."""
    df.write.mode("overwrite").parquet(destination_path)