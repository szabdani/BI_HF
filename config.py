import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

# .env fájl betöltése
load_dotenv()

# PostgreSQL adatok betöltése
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

# Connection String összeállítása
DATABASE_URI = f'postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'

# API beállítások
FD_API_KEY = os.getenv("FD_API_KEY")
TM_API_URL = os.getenv("TM_API_URL")

def get_db_engine():
    if not DB_PASSWORD or not DB_USER:
        raise ValueError("Hiányzó adatbázis konfiguráció! Ellenőrizd a .env fájlt.")
        
    return create_engine(DATABASE_URI)