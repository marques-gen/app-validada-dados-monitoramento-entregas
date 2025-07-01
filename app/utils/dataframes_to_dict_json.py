from typing import Dict, List
import pandas as pd
import numpy as np
from schemas.data_contract_pedidos import SCHEMA_PEDIDOS, split_columns_by_type

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

def dataframes_to_dict_json(dataframes: Dict[str, pd.DataFrame]) -> Dict[str, List[dict]]:
    """
    Converte vários dataframes em listas de dicionários (JSON), prontos para envio
    à API, garantindo compatibilidade com o schema.
    """
    expected_columns = list(SCHEMA_PEDIDOS.columns.keys())
    date_columns, numeric_columns, string_columns = split_columns_by_type(SCHEMA_PEDIDOS)

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