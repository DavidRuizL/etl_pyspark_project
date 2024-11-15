from config.config import INPUT_PATH
from config.config import OUTPUT_PATH
from extract.extract_data import extract_data
from transform.transform_data import transform_data
from load.load_data import load_data
from observability.metrics import log_execution_time
from observability.logging_config import check_data_quality

@log_execution_time
def main():
    # Suponiendo que extract_data devuelve una tupla de DataFrames
    raw_data_tuple = extract_data()  # raw_data ahora es una tupla de DataFrames

    # Iterar sobre cada DataFrame en la tupla
    for idx, raw_data in enumerate(raw_data_tuple):
        print(f"Procesando DataFrame {idx + 1}...")
        check_data_quality(raw_data)
        
        # ETL: Transform
        transformed_data = transform_data(raw_data)

        # ETL: Load
        load_data(transformed_data, f"{OUTPUT_PATH}_table_{idx + 1}.csv")

    print("ETL job completed successfully.")

        
if __name__ == "__main__":
    main()