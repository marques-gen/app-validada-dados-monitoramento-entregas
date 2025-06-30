from typing import Dict, List
import pandas as pd
import numpy as np

def dataframes_para_json_pedidos(dataframes: Dict[str, pd.DataFrame]) -> Dict[str, List[dict]]:
    """
    Converte múltiplos DataFrames em JSON formatado para envio à API de pedidos,
    garantindo compatibilidade com o schema da API.
    
    Args:
        dataframes (dict): {nome_arquivo: dataframe}

    Returns:
        dict: {nome_arquivo: lista_json}
    """

    colunas_esperadas = [
        "ID_Pedido", "Data_Pedido", "Prazo_Entrega_Dias", "Tempo_Transito_Dias",
        "Data_Entrega", "Regiao", "Transportadora", "Status_Pedido", "Avaliacao_Cliente"
    ]

    resultado = {}

    for nome_arquivo, df in dataframes.items():
        df_copy = df.copy()

        # Seleciona e reordena apenas as colunas esperadas
        df_copy = df_copy[colunas_esperadas]

        # Converte colunas de data para string ISO (ou string vazia se inválida)
        for col in ["Data_Pedido", "Data_Entrega"]:
            df_copy[col] = pd.to_datetime(df_copy[col], errors="coerce").dt.strftime('%Y-%m-%d')
            df_copy[col] = df_copy[col].fillna("")

        # Converte numéricos para inteiro (com fallback 0)
        for col in ["Prazo_Entrega_Dias", "Tempo_Transito_Dias", "Avaliacao_Cliente"]:
            df_copy[col] = pd.to_numeric(df_copy[col], errors="coerce").fillna(0).astype(int)

        # Garante que campos string não tenham NaN
        for col in ["ID_Pedido", "Regiao", "Transportadora", "Status_Pedido"]:
            df_copy[col] = df_copy[col].fillna("").astype(str)

        # Remove qualquer valor que ainda possa causar erro no JSON
        df_copy.replace([np.nan, np.inf, -np.inf], "", inplace=True)

        # Converte para lista de dicionários JSON-compliant
        resultado[nome_arquivo] = df_copy.to_dict(orient="records")

    return resultado

