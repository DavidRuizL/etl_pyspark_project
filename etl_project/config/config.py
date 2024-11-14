import yaml
import os

# Ruta por defecto al archivo de configuración YAML
DEFAULT_CONFIG_PATH = "etl_project/config/config.yaml"

def load_config(config_path=DEFAULT_CONFIG_PATH):
    """Carga la configuración desde un archivo YAML."""
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"El archivo de configuración {config_path} no se encuentra.")
    
    with open(config_path, "r") as file:
        config = yaml.safe_load(file)
    
    return config

# Cargar la configuración
config = load_config()

# Asignar valores de configuración a variables globales
INPUT_PATH = config.get("INPUT_PATH", "data/input/datos.csv")  # Valor por defecto si no se encuentra la clave
OUTPUT_PATH = config.get("OUTPUT_PATH", "data/output/datos_transformados.csv")  # Valor por defecto
