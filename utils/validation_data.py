import re
from datetime import datetime
import pandas as pd
import pandera as pa
from pandera import Column, DataFrameSchema
from pandera.errors import SchemaErrors
from pandera import Column, DataFrameSchema, errors

# Regex de nome
NOME_VALIDO_REGEX = r"^base_monitoramento_entregas_(\d{6})\.csv$"

schema = DataFrameSchema({
    "ID_Pedido": Column(pa.String, nullable=False, unique=True),
    "Data_Pedido": Column(pa.String, nullable=False),
    "Prazo_Entrega_Dias": Column(pa.Float64, nullable=False),
    "Tempo_Transito_Dias": Column(pa.Float64, nullable=False),
    "Data_Entrega": Column(pa.String, nullable=True),
    "Regiao": Column(pa.String, nullable=True),
    "Transportadora": Column(pa.String, nullable=False),
    "Status_Pedido": Column(pa.String, nullable=True),
    "Avaliacao_Cliente": Column(pa.Float64, nullable=True),
})

def nome_valido(nome_arquivo: str) -> bool:
    return re.match(NOME_VALIDO_REGEX, nome_arquivo) is not None

def validar_nome_data(nome_arquivo: str) -> bool:
    match = re.match(NOME_VALIDO_REGEX, nome_arquivo)
    if not match:
        return False
    try:
        datetime.strptime(match.group(1), "%Y%m")
        return True
    except ValueError:
        return False

def validar_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    return schema.validate(df, lazy=True)

