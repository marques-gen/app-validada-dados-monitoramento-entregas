import pandas as pd
import pandera
from app.schema import schema
from pathlib import Path
import duckdb

def validar_dados(df: pd.DataFrame) -> bool:
    try:
        schema.validate(df)
        return True
    except pandera.errors.SchemaError as e:
        st.error(f"Erro de validação: {e}")
        return False

def salvar_parquet(df: pd.DataFrame, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    duckdb.sql("INSTALL parquet; LOAD parquet;")
    duckdb.query("CREATE OR REPLACE TABLE temp_table AS SELECT * FROM df")
    duckdb.query(f"COPY temp_table TO '{str(path)}' (FORMAT 'parquet');")


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

