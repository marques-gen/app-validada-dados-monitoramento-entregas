from typing import Dict, List
import pandas as pd
import numpy as np
from schemas.pedidos_schema import pedidos_schema  # Importe o schema

def convert_dates(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    for col in columns:
        df[col] = pd.to_datetime(df[col], errors="coerce").dt.strftime('%Y-%m-%d')
        df[col] = df[col].fillna("")
    return df

def convert_numerics(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    for col in columns:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
    return df

def convert_strings(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    for col in columns:
        df[col] = df[col].fillna("").astype(str)
    return df

def dataframes_to_json_orders(dataframes: Dict[str, pd.DataFrame]) -> Dict[str, List[dict]]:
    """
    Converts multiple DataFrames to JSON format for the orders API,
    ensuring compatibility with the schema.
    """
    # Use columns from the schema
    expected_columns = list(orders_schema.columns.keys())

    date_columns = ["Data_Pedido", "Data_Entrega"]
    numeric_columns = ["Prazo_Entrega_Dias", "Tempo_Transito_Dias", "Avaliacao_Cliente"]
    string_columns = ["ID_Pedido", "Regiao", "Transportadora", "Status_Pedido"]

    result = {}

    for file_name, df in dataframes.items():
        df_copy = df.copy()
        df_copy = df_copy[expected_columns]

        df_copy = convert_dates(df_copy, date_columns)
        df_copy = convert_numerics(df_copy, numeric_columns)
        df_copy = convert_strings(df_copy, string_columns)

        df_copy.replace([np.nan, np.inf, -np.inf], "", inplace=True)
        result[file_name] = df_copy.to_dict(orient="records")

    return result