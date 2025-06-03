import streamlit as st
import pandas as pd
import pandera as pa
from pandera import Column, DataFrameSchema
from dotenv import load_dotenv
import os

# Carrega variáveis do .env
load_dotenv()

# Define esquema de validação
schema = DataFrameSchema({
    "ID_Pedido": Column(int, nullable=False),
    "Data_Pedido": Column(pa.DateTime, nullable=False),
    "Prazo_Entrega_Dias": Column(int, nullable=False),
    "Tempo_Transito_Dias": Column(int, nullable=False),
    "Data_Entrega": Column(pa.DateTime, nullable=True),
    "Regiao": Column(str, nullable=True),
    "Transportadora": Column(str, nullable=False),
    "Status_Pedido": Column(str, nullable=True),
    "Avaliacao_Cliente": Column(str, nullable=True),
})

# Interface Streamlit
st.title("Validação de arquivos")

uploaded_file = st.file_uploader("Faça upload de um arquivo CSV", type="csv")

if uploaded_file is not None:
    try:
        df = pd.read_csv(uploaded_file, parse_dates=["Data_Pedido", "Data_Entrega"])
        schema.validate(df)
        st.success("✅ Arquivo válido!")
        st.dataframe(df)

        if st.button("Salvar em Parquet"):
            output_dir = os.getenv("PARQUET_DIR", "data/ready")
            os.makedirs(output_dir, exist_ok=True)
            file_name = f"{os.path.splitext(uploaded_file.name)[0]}.parquet"
            df.to_parquet(os.path.join(output_dir, file_name), index=False)
            st.success(f"Arquivo salvo em: {output_dir}/{file_name}")

    except Exception as e:
        st.error(f"❌ Erro na validação: {str(e)}")
