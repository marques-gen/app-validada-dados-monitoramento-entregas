FROM python:3.12-slim

# Atualizar o sistema e instalar dependências essenciais
RUN apt-get update && apt-get install -y \
    git \
    build-essential \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Diretório de trabalho
WORKDIR /app

# Copiar e instalar dependências do Python
COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

# Copiar o restante da aplicação
COPY . .

# Expõe a porta padrão do Streamlit
EXPOSE 8501

# Define PYTHONPATH corretamente (opcional — prioridade é do docker-compose/devcontainer.json)
ENV PYTHONPATH=/workspaces/app-validadados-monitoramento-entregas
    

# Comando default
CMD ["streamlit", "run", "app/main2.py", "--server.port=8501", "--server.address=0.0.0.0"]
ENV PYTHONPATH=/app-validadados-monitoramento-entregas

