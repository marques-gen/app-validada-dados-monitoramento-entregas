import io
import zipfile
from pathlib import Path

def exportar_para_zip(dataframes: dict, timestamp: str) -> io.BytesIO:
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
        for nome_arquivo, df in dataframes.items():
            nome_parquet = nome_arquivo.replace(".csv", f"_{timestamp}.parquet")
            parquet_buffer = io.BytesIO()
            df.to_parquet(parquet_buffer, index=False)
            parquet_buffer.seek(0)
            zip_file.writestr(nome_parquet, parquet_buffer.read())
    zip_buffer.seek(0)
    return zip_buffer
