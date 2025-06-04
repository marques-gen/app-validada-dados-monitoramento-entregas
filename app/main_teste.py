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
    "ID_Pedido": Column(pa.String, nullable=False, unique=True), # string
    "Data_Pedido": Column(pa.String, nullable=False),  # DateTime
    "Prazo_Entrega_Dias": Column(pa.Float64, nullable=False), # Int
    "Tempo_Transito_Dias": Column(pa.Int, nullable=False), # Int
    "Data_Entrega": Column(pa.String, nullable=True), # Datetime
    "Regiao": Column(pa.String, nullable=True),
    "Transportadora": Column(pa.String, nullable=False),
    "Status_Pedido": Column(pa.String, nullable=True),
    "Avaliacao_Cliente": Column(pa.Float64, nullable=True), # Int
})

# %%
df_teste=pd.read_csv('../data/raw/base_monitoramento_entregas_202401.csv',delimiter=";")
df_teste.head
# %%
df_teste.dtypes
# %%
