# utils/logs.py

import pandas as pd
from pathlib import Path

def gerar_log_erros(erros_por_arquivo: dict, timestamp: str, caminho_temporario: Path) -> Path:
    logs = []

    for nome_arquivo, erros_colunas in erros_por_arquivo.items():
        for coluna, df_erros in erros_colunas.items():
            for _, row in df_erros.iterrows():
                logs.append({
                    "Arquivo": nome_arquivo,
                    "Coluna": coluna,
                    "Índice": row["index"],
                    "Valor com Erro": row["failure_case"],
                    "Validação": row["check"],
                })

    df_log = pd.DataFrame(logs)
    nome_log = f"log_erros_validacao_{timestamp}.csv"
    caminho_log = caminho_temporario / nome_log
    df_log.to_csv(caminho_log, index=False, sep=";")

    return caminho_log
