from dotenv import load_dotenv
from pathlib import Path
import os

# Carrega .env da raiz do projeto, mesmo se rodar a partir de app/
env_path = Path(__file__).resolve().parents[1] / '.env'
load_dotenv(dotenv_path=env_path)

def get_env_var(var_name: str, required: bool = True) -> str:
    value = os.getenv(var_name)
    if required and (value is None or value.strip() == ""):
        raise ValueError(f"Variável de ambiente '{var_name}' não está definida.")
    return value
