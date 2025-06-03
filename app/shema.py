import pandera as pa
from pandera import Column, DataFrameSchema
from pandera.dtypes import DateTime, Int, String

schema = DataFrameSchema({
    "ID_Pedido": Column(Int, nullable=False,unique=True),
    "Data_Pedido": Column(DateTime, nullable=False),
    "Prazo_Entrega_Dias": Column(Int, nullable=False),
    "Tempo_Transito_Dias": Column(Int, nullable=False),
    "Data_Entrega": Column(DateTime, nullable=True),
    "Regiao": Column(String, nullable=True),
    "Transportadora": Column(String, nullable=False),
    "Status_Pedido": Column(String, nullable=True),
    "Avaliacao_Cliente": Column(String, nullable=True),
})
