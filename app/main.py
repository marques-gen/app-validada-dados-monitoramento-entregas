import streamlit as st
import pandas as pd
import pandera as pa
import re
from pandera import Column, DataFrameSchema, Check
from pandera.errors import SchemaErrors

# Define o schema
schema = DataFrameSchema({
    "ID_Pedido": Column(pa.Int, nullable=False, unique=True),
    "Data_Pedido": Column(pa.DateTime, nullable=False),
    "Prazo_Entrega_Dias": Column(pa.Int, nullable=False),
    "Tempo_Transito_Dias": Column(pa.Int, nullable=False),
    "Data_Entrega": Column(pa.DateTime, nullable=True),
    "Regiao": Column(pa.String, nullable=True),
    "Transportadora": Column(pa.String, nullable=False),
    "Status_Pedido": Column(pa.String, nullable=True),
    "Avaliacao_Cliente": Column(pa.String, nullable=True),
})

# Expressão regular para validar nome do arquivo
nome_valido_regex = r"^base_monitoramento_entregas_\d{6}\.csv$"

# Streamlit App
st.title("📄 Validador de Arquivo CSV com Pandera")

uploaded_file = st.file_uploader("📤 Envie o arquivo CSV", type=["csv"])

if uploaded_file:
    filename = uploaded_file.name

    # Verificar se o nome está no padrão
    if not re.match(nome_valido_regex, filename):
        st.error("❌ Nome do arquivo inválido. O nome deve seguir o padrão: `base_monitoramento_entregas_YYYYMM.csv`")
    else:
        try:
            df = pd.read_csv(uploaded_file,delimiter=";")
            st.subheader("📋 Pré-visualização dos dados")
            st.dataframe(df)

            # Validação com Pandera
            validated_df = schema.validate(df, lazy=True)
            st.success("✅ Dados validados com sucesso!")

            # Exibir dados validados
            st.subheader("✅ Dados Validados")
            st.dataframe(validated_df)

            # Botão para exportar
            if st.button("💾 Salvar como Parquet"):
                validated_df.to_parquet("dados_validados.parquet", index=False)
                st.success("📁 Arquivo salvo como 'dados_validados.parquet'")

        except SchemaErrors as e:
            st.error("❌ Erros de validação encontrados nos dados.")
            failure_cases = e.failure_cases.copy()
            st.subheader("📌 Detalhes dos Erros de Validação")
            grouped = failure_cases.groupby("column")
            for col, group in grouped:
                st.markdown(f"### ❗ Coluna: `{col}`")
                st.dataframe(group[["index", "failure_case", "check"]])

        except Exception as e:
            st.error(f"⚠️ Erro inesperado: {str(e)}")
