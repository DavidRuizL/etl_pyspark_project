def load_data(df, destination_path):
    """Carga los datos transformados en el destino especificado."""
    # Configurar Spark para evitar problemas con archivos locales en Windows
    df.sparkSession.conf.set("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")

    print(f"Intentando guardar los datos en: {destination_path}")

    try:
        df.write.mode("overwrite") \
            .option("header", "true") \
            .csv(df.write.parquet(destination_path))
        print(f"Datos guardados exitosamente en {destination_path}")
    except Exception as e:
        print(f"Error al guardar los datos: {e}")
