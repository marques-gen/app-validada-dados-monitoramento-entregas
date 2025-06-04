import streamlit as st
import pandas as pd
import pandera as pa
from pandera import Column, DataFrameSchema
from pandera.errors import SchemaErrors
import re
from datetime import datetime

# Remove o parâmetro de reset se estiver presente
if "reset" in st.query_params:
    st.query_params.clear()


# Regex para validar nome do arquivo
NOME_VALIDO_REGEX = r"^base_monitoramento_entregas_(\d{6})\.csv$"

# Schema esperado
schema = DataFrameSchema({
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

# Título e instruções
st.set_page_config(page_title="Validador CSV", layout="centered")
st.title("🧪 Validador de Arquivos CSV - Lote com Nome + Esquema")
st.markdown("📥 Envie um ou mais arquivos CSV com o nome no padrão: `base_monitoramento_entregas_YYYYMM.csv`")

# Estado inicial
if "parquet_salvo" not in st.session_state:
    st.session_state.parquet_salvo = False

# Upload
uploaded_files = st.file_uploader(
    label="Selecione os arquivos CSV",
    type=["csv"],
    accept_multiple_files=True
)

arquivos_validos = {}
erros_por_arquivo = {}

# Validação de arquivos
if uploaded_files:
    for file in uploaded_files:
        st.divider()
        st.subheader(f"📄 Verificando arquivo: `{file.name}`")

        match = re.match(NOME_VALIDO_REGEX, file.name)
        if not match:
            st.error("❌ Nome inválido. Esperado: `base_monitoramento_entregas_YYYYMM.csv`")
            continue

        yyyymm = match.group(1)
        try:
            datetime.strptime(yyyymm, "%Y%m")
        except ValueError:
            st.error(f"❌ Data inválida no nome: `{yyyymm}`")
            continue

        try:
            df = pd.read_csv(file, delimiter=";")
            validated_df = schema.validate(df, lazy=True)
            st.success("✅ Arquivo válido")
            st.dataframe(validated_df.head())
            arquivos_validos[file.name] = validated_df

        except SchemaErrors as e:
            st.error("❌ Erros de validação encontrados:")
            failure_df = e.failure_cases
            erros_por_coluna = {
                col: failure_df[failure_df["column"] == col][["index", "failure_case", "check"]].reset_index(drop=True)
                for col in failure_df["column"].unique()
            }
            erros_por_arquivo[file.name] = erros_por_coluna

        except Exception as e:
            st.error(f"❌ Erro inesperado ao processar o arquivo: {e}")

# Exibe erros detalhados
if erros_por_arquivo:
    st.subheader("📌 Detalhamento dos erros por arquivo e coluna")
    for arquivo, colunas in erros_por_arquivo.items():
        st.markdown(f"### 📂 Arquivo: `{arquivo}`")
        for coluna, erros in colunas.items():
            st.markdown(f"**🔸 Coluna: `{coluna}`**")
            st.dataframe(erros)

# Inicializa o estado do botão e arquivos, se ainda não existirem
if "parquet_salvo" not in st.session_state:
    st.session_state.parquet_salvo = False

# Exibe botão e salva arquivos válidos
if arquivos_validos and not st.session_state.parquet_salvo:
    if st.button("💾 Salvar arquivos válidos como Parquet"):
        for nome, df in arquivos_validos.items():
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            nome_parquet = nome.replace(".csv", f"_{timestamp}.parquet")
            df.to_parquet(nome_parquet, index=False)

        st.success("🎉 Arquivos salvos com sucesso!")

        # Marca como salvo
        st.session_state.parquet_salvo = True

        # Força limpeza da URL e reinício da página
        st.query_params.clear()
        st.rerun()

