
from sqlalchemy import create_engine
from app.config import get_env_var

# Caminho absoluto até a raiz do projeto (onde está o .env)
#env_path = Path(__file__).resolve().parents[1] / ".env"
#load_dotenv(dotenv_path=env_path)

def get_engine():
    user = get_env_var("POSTGRES_USER")
    password = get_env_var("POSTGRES_PASSWORD")
    host = get_env_var("POSTGRES_HOST")
    port = get_env_var("POSTGRES_PORT")
    db = get_env_var("POSTGRES_DB")

    try:
        int(port)  # valida a porta
    except ValueError:
        raise ValueError(f"POSTGRES_PORT ('{port}') não é um número válido.")

    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    return create_engine(url)
