import streamlit as st
import pandas as pd
import pandera as pa
from pandera import Column, DataFrameSchema
from pandera.errors import SchemaErrors
import re
from datetime import datetime

# Regex para validar nome do arquivo: base_monitoramento_entregas_YYYYMM.csv
NOME_VALIDO_REGEX = r"^base_monitoramento_entregas_(\d{6})\.csv$"

# Schema esperado
schema = DataFrameSchema({
    "ID_Pedido": Column(pa.String, nullable=False, unique=True),
    "Data_Pedido": Column(pa.DateTime, nullable=False),
    "Prazo_Entrega_Dias": Column(pa.Int, nullable=False),
    "Tempo_Transito_Dias": Column(pa.Int, nullable=False),
    "Data_Entrega": Column(pa.DateTime, nullable=True),
    "Regiao": Column(pa.String, nullable=True),
    "Transportadora": Column(pa.String, nullable=False),
    "Status_Pedido": Column(pa.String, nullable=True),
    "Avaliacao_Cliente": Column(pa.Int, nullable=True),
})


df_teste=pd.read_csv(r'C:\Users\User\Downloads\painel_monitoramento_entregas\src\raw\base_monitoramento_entregas_202401.csv')
df_teste.head

st.title("🧪 Validador de Arquivos CSV - Lote com Nome + Esquema")

uploaded_files = st.file_uploader(
    "📥 Envie um ou mais arquivos CSV com o nome no padrão: base_monitoramento_entregas_YYYYMM.csv",
    type=["csv"],
    accept_multiple_files=True
)

arquivos_validos = {}
erros_por_arquivo = {}

if uploaded_files:
    for file in uploaded_files:
        st.divider()
        st.subheader(f"📄 Verificando arquivo: `{file.name}`")

        match = re.match(NOME_VALIDO_REGEX, file.name)

        if not match:
            st.error("❌ Nome inválido. Esperado: `base_monitoramento_entregas_YYYYMM.csv`")
            continue

        # Extração e validação de data no nome
        yyyymm = match.group(1)
        try:
            datetime.strptime(yyyymm, "%Y%m")
        except ValueError:
            st.error(f"❌ Mês inválido no nome do arquivo: `{yyyymm}` não é uma data válida.")
            continue

        try:
            df = pd.read_csv(file,delimiter=";")
            validated_df = schema.validate(df, lazy=True)

            st.success("✅ Arquivo válido")
            st.dataframe(validated_df.head())
            arquivos_validos[file.name] = validated_df

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

    # Exportar arquivos válidos
    if arquivos_validos and st.button("💾 Salvar arquivos válidos como Parquet"):
        for nome, df in arquivos_validos.items():
            parquet_name = nome.replace(".csv", ".parquet")
            df.to_parquet(parquet_name, index=False)
        st.success("🎉 Arquivos salvos com sucesso!")
