import re
from datetime import datetime
import pandas as pd
import pandera as pa
from pandera import Column, DataFrameSchema
from pandera.errors import SchemaErrors
from pandera import Column, DataFrameSchema, errors
from schemas.data_contract_pedidos import SCHEMA_PEDIDOS, filename_regex, split_columns_by_type 


def validar_nome(nome_arquivo: str) -> bool:
    return re.match(filename_regex, nome_arquivo) is not None

def validar_nome_data(nome_arquivo: str) -> bool:
    match = re.match(filename_regex, nome_arquivo)
    if not match:
        return False
    try:
        print(f"Nome do arquivo: {nome_arquivo}, match: {match}")
        datetime.strptime(match.group(1), "%Y%m")
        return True
    except ValueError:
        return False

def validar_dataframe(df: pd.DataFrame):
    """
    Valida o DataFrame usando o schema Pandera.
    Retorna (df_validado, None) se válido,
    ou (None, erros_por_coluna) se inválido.
    """
    try:
        df_validado = SCHEMA_PEDIDOS.validate(df, lazy=True)
        return df_validado, None
    except pa.errors.SchemaErrors as e:
        failure_df = e.failure_cases
        erros_por_coluna = {}
        for col in failure_df["column"].unique():
            col_erros = failure_df[failure_df["column"] == col][["index", "failure_case", "check"]]
            erros_por_coluna[col] = col_erros.reset_index(drop=True)
        return None, erros_por_coluna

# Para obter as colunas por tipo:
date_cols, numeric_cols, string_cols = split_columns_by_type(SCHEMA_PEDIDOS)



