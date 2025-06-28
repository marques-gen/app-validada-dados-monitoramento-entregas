
from sqlalchemy import create_engine
from config.env_loader import get_env_var


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
