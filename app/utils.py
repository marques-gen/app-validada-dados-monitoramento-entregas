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
