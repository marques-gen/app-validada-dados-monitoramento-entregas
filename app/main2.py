import streamlit as st
import pandas as pd
import pandera as pa
from pandera import Column, DataFrameSchema
from pandera.errors import SchemaErrors
from datetime import datetime
from zoneinfo import ZoneInfo
from utils.validation_data import nome_valido, validar_nome_data, validar_dataframe
from utils.database import get_engine
from utils.export_data import exportar_para_zip
from utils.converter import dataframes_para_json_pedidos
import requests
import sys
import os

sys.path.append(os.path.dirname(__file__))


# Setup inicial
st.set_page_config("Validador CSV", layout="wide")
st.title("🧪 Validador de Arquivos CSV - Upload + Validação + Exportação")

# Inicializa sessão
if "uploader_key" not in st.session_state:
    st.session_state["uploader_key"] = 0

if "timestamp_exportacao" not in st.session_state:
    st.session_state.timestamp_exportacao = datetime.now(ZoneInfo("America/Sao_Paulo")).strftime("%Y%m%d_%H%M%S")

uploaded_files = st.file_uploader(
    "📥 Envie arquivos .csv com padrão `base_monitoramento_entregas_YYYYMM.csv`",
    type=["csv"], accept_multiple_files=True,
    key=f"uploader_{st.session_state['uploader_key']}"
)

arquivos_validos = {}
erros_por_arquivo = {}

if uploaded_files:
    for file in uploaded_files:
        st.divider()
        st.subheader(f"📄 Verificando: `{file.name}`")

        if not nome_valido(file.name):
            st.error("❌ Nome inválido.")
            continue

        if not validar_nome_data(file.name):
            st.error("❌ Data inválida no nome do arquivo.")
            continue

        try:
            df = pd.read_csv(file, delimiter=";")
            validado = validar_dataframe(df)
            arquivos_validos[file.name] = validado
            st.success("✅ Arquivo válido.")

        except SchemaErrors as e:
            st.error("❌ Erros de validação encontrados:")

            failure_df = e.failure_cases
            erros_por_coluna = {}

            for col in failure_df["column"].unique():
                col_erros = failure_df[failure_df["column"] == col][["index", "failure_case", "check"]]
                erros_por_coluna[col] = col_erros.reset_index(drop=True)

            erros_por_arquivo[file.name] = erros_por_coluna
            
        except Exception as e:
            st.error(f"❌ Erro inesperado ao processar o arquivo: {e}")

    # Exibe os erros organizados por arquivo e por coluna
    if erros_por_arquivo:
        st.subheader("❌ Detalhamento dos erros por arquivo e coluna")
        for nome_arquivo, erros_colunas in erros_por_arquivo.items():
            st.markdown(f"### 📂 Arquivo: `{nome_arquivo}`")
            for coluna, erros_df in erros_colunas.items():
                st.markdown(f"**🔸 Coluna com erro: `{coluna}`**")
                st.dataframe(erros_df)


    # Se todos forem válidos
    if len(arquivos_validos) == len(uploaded_files):
        st.success("🎉 Todos os arquivos são válidos!")

        acao = st.radio("Deseja:", ["⬇️ Fazer download dos Parquets", "📥 Inserir no PostgreSQL","📤 Enviar para API"])

        if acao == "⬇️ Fazer download dos Parquets":
            zip_buffer = exportar_para_zip(arquivos_validos, st.session_state.timestamp_exportacao)
            st.download_button(
                label="⬇️ Baixar ZIP dos Parquets",
                data=zip_buffer,
                file_name="arquivos_validos.zip",
                mime="application/zip"
            )

        elif acao == "📥 Inserir no PostgreSQL":
            def inserir_dados():
                engine = get_engine()
                insercao_ok = True

                for nome_arquivo, df in arquivos_validos.items():
                    df["dt_exec"] = st.session_state.timestamp_exportacao
                    try:
                        df.to_sql("tabela_destino", con=engine, index=False, if_exists="append")
                        st.success(f"✅ `{nome_arquivo}` inserido com sucesso.")
                    except Exception as e:
                        st.error(f"❌ Erro ao inserir `{nome_arquivo}`: {e}")
                        insercao_ok = False

                # Se tudo ok, força recarregamento e limpeza
                if insercao_ok:
                    st.session_state["uploader_key"] += 1
                    st.rerun()

            st.button("📥 Inserir arquivos no banco", on_click=inserir_dados)

        elif acao == "📤 Enviar para API":

            def enviar_para_api():
                url = "http://api-fastapi-template:8000/pedidos/"
                sucesso_total = True

                # Converte para o formato JSON esperado pela API
                json_arquivos = dataframes_para_json_pedidos(arquivos_validos)

                for nome_arquivo, registros_json in json_arquivos.items():
                    try:
                        response = requests.post(url, json=registros_json)

                        if response.status_code == 200:
                            st.success(f"✅ `{nome_arquivo}` enviado com sucesso.")
                        else:
                            st.error(f"❌ Falha ao enviar `{nome_arquivo}`. Código: {response.status_code} | Erro: {response.text}")
                            sucesso_total = False

                    except Exception as e:
                        st.error(f"❌ Erro ao enviar `{nome_arquivo}`: {e}")
                        sucesso_total = False

                if sucesso_total:
                    st.session_state["uploader_key"] += 1
                    st.rerun()                    
            st.button("📤 Enviar arquivos para API", on_click=enviar_para_api)


    else:
        st.warning("⚠️ O download ou carga só serão permitidos se todos os arquivos forem válidos.")

