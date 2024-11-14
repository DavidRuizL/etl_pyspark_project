def validate_schema(df, expected_columns):
    """Verifica si el DataFrame tiene las columnas esperadas."""
    df_columns = set(df.columns)
    missing_columns = expected_columns - df_columns
    if missing_columns:
        raise ValueError(f"Faltan columnas: {missing_columns}")