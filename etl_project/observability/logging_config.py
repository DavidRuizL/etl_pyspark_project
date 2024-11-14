import logging

logging.basicConfig(level=logging.INFO)

def send_alert(message):
    """Envía una alerta (ejemplo: log de error)."""
    logging.error(f"ALERT: {message}")

def check_data_quality(df):
    """Verifica la calidad de los datos y envía una alerta si es necesario."""
    if df.isEmpty():
        send_alert("Data quality issue: DataFrame is empty.")