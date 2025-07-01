# main2.py

import streamlit as st
from datetime import datetime
from zoneinfo import ZoneInfo
from utils.files_validation import validar_nome, validar_nome_data, validar_dataframe
from config.database_connection import get_engine
from utils.dataframes_to_zip import exportar_para_zip
from utils.dataframes_to_dict_json import dataframes_to_dict_json
import pandas as pd
import requests

# Setup inicial, configuração da página.
def setup_page():
    st.set_page_config("Validador CSV", layout="wide")
    st.title("🧪 Validador de Arquivos CSV - Upload + Validação + Exportação")

# inicialização de variáveis de sessão
def init_session():
    if "uploader_key" not in st.session_state:
        st.session_state["uploader_key"] = 0
    if "timestamp_exportacao" not in st.session_state:
        st.session_state.timestamp_exportacao = datetime.now(ZoneInfo("America/Sao_Paulo")).strftime("%Y%m%d_%H%M%S")

# inputs usuários para arquivos .csv
def show_file_uploader():
    return st.file_uploader(
        "📥 Envie arquivos .csv com padrão `base_monitoramento_entregas_YYYYMM.csv`",
        type=["csv"], accept_multiple_files=True,
        key=f"uploader_{st.session_state['uploader_key']}"
    )
# função para validar os arquivos enviados
def validate_files(uploaded_files):
    arquivos_validos = {}
    erros_por_arquivo = {}
    for file in uploaded_files:
        st.divider()
        st.subheader(f"📄 Verificando: `{file.name}`")
        if not validar_nome(file.name):
            st.error("❌ Nome inválido.")
            continue
        if not validar_nome_data(file.name):
            st.error("❌ Data inválida no nome do arquivo.")
            continue
        try:
            df = pd.read_csv(file, delimiter=";")
            df_validado, erros_coluna= validar_dataframe(df)
            
            if df_validado is not None:
                arquivos_validos[file.name]=df_validado
                st.success("✅ Arquivo válido.")
            else:
                erros_por_arquivo[file.name] = erros_coluna
        except Exception as e:
            st.error(f"❌ Erro ao processar `{file.name}`: {e}")
            erros_por_arquivo[file.name] = str(e)
    return arquivos_validos, erros_por_arquivo

# função para exibir detalhes dos erros encontrados
def show_error_details(erros_por_arquivo):
    if erros_por_arquivo:
        st.subheader("❌ Detalhamento dos erros por arquivo e coluna")
        for nome_arquivo, erros in erros_por_arquivo.items():
            st.markdown(f"### 📂 Arquivo: `{nome_arquivo}`")
            if isinstance(erros, dict):
                for coluna, erros_df in erros.items():
                    st.markdown(f"**🔸 Coluna com erro: `{coluna}`**")
                    st.dataframe(erros_df)
            else:
                st.error(erros)

# Ações de usúario após validação dos arquivos
def show_actions(arquivos_validos):
    acao = st.radio("Deseja:", ["⬇️ Fazer download dos Parquets", "📥 Inserir no PostgreSQL", "📤 Enviar para API"])
    if acao == "⬇️ Fazer download dos Parquets":
        zip_buffer = exportar_para_zip(arquivos_validos, st.session_state.timestamp_exportacao)
        st.download_button(
            label="⬇️ Baixar ZIP dos Parquets",
            data=zip_buffer,
            file_name="arquivos_validos.zip",
            mime="application/zip"
        )
    elif acao == "📥 Inserir no PostgreSQL":
        st.button("📥 Inserir arquivos no banco", on_click=lambda: inserir_dados(arquivos_validos))
    elif acao == "📤 Enviar para API":
        st.button("📤 Enviar arquivos para API", on_click=lambda: enviar_para_api(arquivos_validos))

def inserir_dados(arquivos_validos):
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
    if insercao_ok:
        st.session_state["uploader_key"] += 1
        st.rerun()

def enviar_para_api(arquivos_validos):
    url = "http://localhost:8000/pedidos/"
    sucesso_total = True
    json_arquivos = dataframes_to_dict_json(arquivos_validos)
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

def main():
    setup_page()
    init_session()
    uploaded_files = show_file_uploader()
    if uploaded_files:
        arquivos_validos, erros_por_arquivo = validate_files(uploaded_files)
        show_error_details(erros_por_arquivo)
        if len(arquivos_validos) == len(uploaded_files):
            st.success("🎉 Todos os arquivos são válidos!")
            show_actions(arquivos_validos)
        else:
            st.warning("⚠️ O download ou carga só serão permitidos se todos os arquivos forem válidos.")

if __name__ == "__main__":
    main()