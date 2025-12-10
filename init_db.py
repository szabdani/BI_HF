from config import get_db_engine
from models import Base
from sqlalchemy.orm import Session
from sqlalchemy.sql import text

def init_db():
    engine = get_db_engine()

    sql_drop_cascade = text("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")    
    with Session(engine) as session:
        try:
            session.execute(sql_drop_cascade)
            session.commit()
            print("A korábbi táblák (és a séma) sikeresen törölve.")
        except Exception as e:
            session.rollback()
            print(f"Hiba a DROP CASCADE parancs futtatásakor: {e}")
            print("Folytatjuk a create_all paranccsal.")

    print("Adatbázis táblák létrehozása...")
    Base.metadata.create_all(engine)
    print("A táblák létrejöttek a football_dwh adatbázisban.")

if __name__ == "__main__":
    init_db()
