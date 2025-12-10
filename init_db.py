from config import get_db_engine
from models import Base

def init_db():
    engine = get_db_engine()
    print("Minden létező tábla törlése a sémában")
    try:
        Base.metadata.drop_all(engine)
        print("A korábbi táblák sikeresen törölve.")
    except Exception as e:
        print(f"Váratlan hiba a törlés során: {e}")

    print("Adatbázis táblák létrehozása...")
    Base.metadata.create_all(engine)
    print("A táblák létrejöttek a football_dwh adatbázisban.")

if __name__ == "__main__":
    init_db()