from config.config import load_config
from utils.spark_session import create_spark_session
from extract.extract_data import extract_data
from transform.transform_data import transform_data
from load.load_data import load_data
from observability.metrics import log_execution_time

@log_execution_time
def main():
    # Cargar configuración
    config = load_config()
    
    # Crear sesión de Spark
    spark = create_spark_session()

    # ETL: Extract
    raw_data = extract_data(spark, config["data_source"])

    # ETL: Transform
    transformed_data = transform_data(raw_data)

    # ETL: Load
    load_data(transformed_data, config["data_destination"])

    print("ETL job completed successfully.")

if __name__ == "__main__":
    main()