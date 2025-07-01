import pandera as pa
from pandera import Column, DataFrameSchema

SCHEMA_PEDIDOS = DataFrameSchema({
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

filename_regex = r"^base_monitoramento_entregas_(\d{6})\.csv$"

def split_columns_by_type(schema: pa.DataFrameSchema):
    """Retorna listas de colunas separadas por tipo."""
    
    date_columns = []
    numeric_columns = []
    string_columns = []

    for col_name, col in schema.columns.items():
        dtype = str(col.dtype).lower()
        if "datetime" in dtype or "date" in dtype:
            date_columns.append(col_name)
        elif "float" in dtype or "int" in dtype:
            numeric_columns.append(col_name)
        elif "str" in dtype or "string" in dtype:
            string_columns.append(col_name)
    return date_columns, numeric_columns, string_columns


