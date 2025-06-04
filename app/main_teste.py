# %%
import pandas as pd
import pandera as pa
from pandera import Column, DataFrameSchema
from pandera.errors import SchemaErrors
import re
from datetime import datetime

# Regex para validar nome do arquivo: base_monitoramento_entregas_YYYYMM.csv
NOME_VALIDO_REGEX = r"^base_monitoramento_entregas_(\d{6})\.csv$"
# %%
# Schema esperado
schema = DataFrameSchema({
    "ID_Pedido": Column(pa.String, nullable=False, unique=True),
    "Data_Pedido": Column(pa.DateTime, nullable=False),
    "Prazo_Entrega_Dias": Column(pa.Int, nullable=False),
    "Tempo_Transito_Dias": Column(pa.Int, nullable=False),
    "Data_Entrega": Column(pa.DateTime, nullable=True),
    "Regiao": Column(pa.String, nullable=True),
    "Transportadora": Column(pa.String, nullable=False),
    "Status_Pedido": Column(pa.String, nullable=True),
    "Avaliacao_Cliente": Column(pa.Int, nullable=True),
})

# %%
df_teste=pd.read_csv('../data/raw/base_monitoramento_entregas_202401.csv',delimiter=";")
df_teste.head
# %%
df_teste.dtypes
# %%
