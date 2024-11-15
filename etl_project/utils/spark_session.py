from pyspark.sql import SparkSession

def get_spark_session(app_name="ETL Application"):
    """
    Crea y devuelve una sesión de Spark configurada para evitar el uso de bibliotecas nativas de Hadoop.
    """
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
        .config("spark.hadoop.io.nativeio.NativeIO", "false") \
        .config("spark.hadoop.fs.native.lib", "false") \
        .config("spark.hadoop.fs.local", "true") \
        .config("spark.hadoop.fs.local.blocksize", "134217728") \
        .config("spark.hadoop.fs.file.impl.disable.cache", "true") \
        .config("spark.hadoop.fs.hdfs.impl.disable.cache", "true") \
        .getOrCreate()

    return spark
