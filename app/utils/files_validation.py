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

def validar_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    return SCHEMA_PEDIDOS.validate(df, lazy=True)

# Para obter as colunas por tipo:
date_cols, numeric_cols, string_cols = split_columns_by_type(SCHEMA_PEDIDOS)

def validar_arquivos_enviados(uploaded_files):
    arquivos_validos = {}
    erros_por_arquivo = {}

    for file in uploaded_files:
        if not validar_nome(file.name):
            erros_por_arquivo[file.name] = {"__file__": "Nome inválido."}
            continue

        if not validar_nome_data(file.name):
            erros_por_arquivo[file.name] = {"__file__": "Data inválida no nome do arquivo."}
            continue

        try:
            df = pd.read_csv(file, delimiter=";")
            validado = validar_dataframe(df)
            arquivos_validos[file.name] = validado
        except SchemaErrors as e:
            failure_df = e.failure_cases
            erros_por_coluna = {}
            for col in failure_df["column"].unique():
                col_erros = failure_df[failure_df["column"] == col][["index", "failure_case", "check"]]
                erros_por_coluna[col] = col_erros.reset_index(drop=True)
            erros_por_arquivo[file.name] = erros_por_coluna
        except Exception as e:
            erros_por_arquivo[file.name] = {"__file__": f"Erro inesperado: {e}"}

    return arquivos_validos, erros_por_arquivo


