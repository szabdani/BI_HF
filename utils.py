import time
import requests
import logging
from datetime import date, datetime
from sqlalchemy.orm import sessionmaker
from config import get_db_engine, FD_API_KEY, TM_API_URL
from models import (
    DimSeason, DimCompetition, DimTeam, DimPlayer, FactMatch
)
import re

# --- KONFIGURÁCIÓ ÉS LOGOLÁS ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

engine = get_db_engine()
Session = sessionmaker(bind=engine)
session = Session()

FD_HEADERS = {'X-Auth-Token': FD_API_KEY}

# --- SEGÉDFÜGGVÉNYEK ---
def get_db_session():
    return Session()

def requests_get_retry(url, headers=None, retries=3, backoff=5):
    """Biztonságos kérés újrapróbálkozással."""
    for i in range(retries):
        try:
            response = requests.get(url, headers=headers)
            if response.status_code in [200, 404]: # A 404 is válasz, csak nincs adat
                return response
            elif response.status_code == 429: # Too Many Requests
                logger.warning(f"Rate Limit (429) a {url}-en. Várakozás {backoff} mp...")
                time.sleep(backoff)
            else:
                logger.warning(f"Hiba ({response.status_code}) a {url}-en. Újrapróbálkozás ({i+1}/{retries})...")
        except Exception as e:
            logger.error(f"Kivétel történt: {e}")
        
        time.sleep(backoff)
    return None

def get_season_from_TMname(season_name_tm):
    """
    Szezon lekérése a neve alapján (pl. '22/23' vagy '2022').
    """
    logger.info(f"Szezon lekérése TM név alapján: {season_name_tm}")

    if not season_name_tm or not isinstance(season_name_tm, str):
        logger.error(f"Érvénytelen bemenet a szezonnévhez: {season_name_tm}")
        return None

    season_code = season_name_tm.strip()
    
    start_year = None
    end_year = None
    name = None

    try:
        # Ellenőrizzük az "XX/YY" formátumot (pl. '22/23' vagy '98/99')
        if re.match(r'^\d{2}/\d{2}$', season_code):
            
            start_two_digits = int(season_code.split('/')[0])
            end_two_digits = int(season_code.split('/')[1])
            
            # Küszöbérték logika (50 felett 19xx, alatt 20xx)
            THRESHOLD = 50 
            
            if start_two_digits >= THRESHOLD:
                start_year = 1900 + start_two_digits
                end_year = 1900 + end_two_digits
            else:
                start_year = 2000 + start_two_digits
                end_year = 2000 + end_two_digits
            
            name = f"{start_year}/{end_year}"

        # Ha nem, akkor ellenőrizzük az "XXXX" formátumot (pl. '2022')
        elif re.match(r'^\d{4}$', season_code):
            start_year = int(season_code)
            end_year = start_year + 1
            name = f"{start_year}/{end_year}" 
        
        # Ha sikerült azonosítani, létrehozzuk/lekérjük a szezont
        if start_year and end_year and name:
            season = get_or_create_season(name, start_year, end_year)
            return season
        else:
            logger.warning(f"Szezonkód nem azonosítható: {season_code}")
            return None

    except Exception as e:
        logger.error(f"Hiba a szezonkód feldolgozásánál ({season_code}): {e}")
        return None

# --- DB lekérdezések ---
def get_or_create_season(name, start_year, end_year):
    """
    Megkeresi a szezont a DB-ben, ha nincs készít.
    """
    season = session.query(DimSeason).filter_by(name=name).first()
    if not season:
        season_name_TM = f"{str(start_year)[-2:]}/{str(end_year)[-2:]}"
        season = DimSeason(name=name, season_name_TM=season_name_TM, start_year=start_year, end_year=end_year)
        session.add(season)
        session.commit()
        logger.info(f"Szezon létrehozva: {name}")
    return season

def get_or_create_competition(fd_code, name, emblem_url):
    """
    Megkeresi a bajnokságot a DB-ben, ha nincs készít.
    """
    comp = session.query(DimCompetition).filter_by(fd_code=fd_code).first()
    if not comp:
        logger.info(f"Új bajnokság létrehozása: {name}...")
        
        comp = DimCompetition(
            fd_id=fd_code, 
            name=name, 
            emblem_url=emblem_url
        )

        # TM API hívás a hiányzó adatok megszerzésére
        tm_data = fetch_tm_competition_data(name)
        if tm_data:
            exists = session.query(DimCompetition).filter_by(tm_id=tm_data.get('id')).first()
            if exists:
                logger.info(f"Ez a bajnokság már létezik a DB-ben: {exists.name}, FD infókkal kiegésztjük.")
                exists.fd_id = comp.fd_id
                exists.emblem_url = comp.emblem_url
                session.commit()
                return exists
            
            comp.tm_id = tm_data.get('id')
            comp.country = tm_data.get('country')
            comp.continent = tm_data.get('continent')

        session.add(comp)
        session.commit()
        logger.info(f"Bajnokság elmentve: {name} (TM ID: {comp.tm_id})")
        
    return comp

def get_or_create_competition_by_tm_id(tm_id, name):
    """
    Megkeresi a bajnokságot TM ID alapján, ha nincs készít.
    """
    comp = session.query(DimCompetition).filter_by(tm_id=tm_id).first()
    if not comp:
        logger.info(f"Hiányzó bajnokság létrehozása TM ID alapján: {name} (ID: {tm_id})...")
        
        comp = DimCompetition(
            fd_id=None, 
            tm_id=tm_id,
            name=name
        )

        # TM API hívás a hiányzó adatok megszerzésére
        tm_data = fetch_tm_competition_data(name)
        if tm_data:
            comp.country = tm_data.get('country')
            comp.continent = tm_data.get('continent')

        session.add(comp)
        session.commit()
        logger.info(f"Bajnokság elmentve: {name} (TM ID: {comp.tm_id})")
        
    return comp

def get_or_create_player(tm_id):
    """
    Megkeresi a játékost a DB-ben, ha nincs készít.
    """
    player = session.query(DimPlayer).filter_by(tm_id=tm_id).first()

    # Diogo Jota elhunyt játékos, akire nem működik a TM API profile hívás
    if(tm_id == "340950" or tm_id == "503866"):    
        logger.warning(f"TM API hiba miatt a játékos kihagyva: (ID: {tm_id})...")
        return player

    if not player:
        logger.info(f"Új játékos feldolgozása: (ID: {tm_id})...")    
    
        # TM Adatok lekérése profil és keresés alapján
        player_data = fetch_tm_player_profile(tm_id)
        player_search_data = fetch_tm_player_search(tm_id, player_data['name'])

        nationalities = player_search_data.get('nationalities')
        if nationalities and isinstance(nationalities, list) and len(nationalities) > 0:
            main_nationality = nationalities[0]
        else:
            main_nationality = None
        
        # Megkeressük a tm_ID-hoz tartozó DimTeam ID-t
        team_id = None
        tm_club_id = player_search_data.get('club').get('id')
        if tm_club_id:
            tm_club_name = player_search_data.get('club').get('name')
            team_id = get_or_create_team_by_tm_id(tm_club_id, tm_club_name)
            
        player = DimPlayer(
            name=player_data['name'],
            tm_id=tm_id,
            position=player_search_data.get('position'),  
            position_name=player_data.get('position').get('main'),
            nationality=main_nationality,
            age=player_search_data.get('age'),    
            shirt_number=player_data.get('shirtNumber'),
            current_team_id=team_id,
        )

        if internal_team_id is None:
            logger.warning(f"Játékos {player_data['name']} mentése csapat-hivatkozás nélkül.")
        
        session.add(player)
        session.commit()
        logger.info(f"Új játékos commitolva: (ID: {tm_id})...")  
    
    return player

def get_or_create_team(fd_team_data, competition_id):
    """
    Ellenőrzi, hogy a csapat létezik-e. Ha nem, létrehozza FD + TM adatokból.
    """
    fd_id = fd_team_data['id']
    team = session.query(DimTeam).filter_by(fd_id=fd_id).first()
    
    if not team:
        logger.info(f"Új csapat feldolgozása: {fd_team_data['name']}...")
        
        # TM Adatok lekérése
        tm_data = fetch_tm_team_data_search(fd_team_data['name'], fd_team_data.get('shortName'))
        
        team = DimTeam(
            fd_id=fd_id,
            name=fd_team_data['name'],
            short_name=fd_team_data.get('shortName'),
            tla=fd_team_data.get('tla'),
            crest_url=fd_team_data.get('crest'),
            competition_id=competition_id
        )
        
        if tm_data:
            team.tm_id = tm_data.get('id')
            # Ha már létezik a csapat TM ID alapján, frissítjük az FD adatokat
            existing_team = session.query(DimTeam).filter_by(tm_id=team.tm_id).first()
            if existing_team:
                logger.info(f"Ez a csapat már létezik a DB-ben: {existing_team.name}, FD infókkal kiegésztjük.")
                existing_team.fd_id = team.fd_id
                existing_team.short_name = team.short_name
                existing_team.tla = team.tla
                existing_team.crest_url = team.crest_url
                existing_team.competition_id = team.competition_id
                session.commit()

                return existing_team

            # TM Csapat profil lekérése a hiányzó adatokért
            club_data = fetch_tm_club_profile(team.tm_id)
            if club_data:
                team.founded = club_data.get('foundedOn')
                team.stadium = club_data.get('stadiumName')
                team.currentTransferRecord = club_data.get('currentTransferRecord')
                team.currentMarketValue = club_data.get('currentMarketValue')
            
        session.add(team)
        session.commit()
        logger.info(f"Új csapat commitolva: {fd_team_data['name']}...")
    
    return team

def get_or_create_team_by_tm_id(tm_id, club_name):
    """
    Megkeresi a csapatot TM ID alapján, ha nincs készít.
    """
    # Megkeressük a tm_ID-hoz tartozó DimTeam ID-t
    team_mapping = session.query(DimTeam).filter_by(tm_id=tm_id).first()
    team_id = None
    if team_mapping:
        team_id = team_mapping.team_id 
    else:
        # Ha nincs a DB-ben, létrehozzuk a csapatot csak TM adatokból
        logger.info(f"Hiányzó csapat ({club_name}, ID: {tm_id}) létrehozása...")
            
        new_tm_team = DimTeam(
            fd_id=None, # Nincs FD ID
            tm_id=tm_id,
            name=club_name,
            competition_id=None 
        )
            
        # Megpróbáljuk a részleteket lekérni
        club_data = fetch_tm_club_profile(tm_id)
        if club_data:
            new_tm_team.founded = club_data.get('foundedOn')
            new_tm_team.stadium = club_data.get('stadiumName')
            new_tm_team.currentTransferRecord = club_data.get('currentTransferRecord')
            new_tm_team.currentMarketValue = club_data.get('currentMarketValue')
            if new_tm_team.name is None:
                new_tm_team.name = club_data.get('name')

        try:
            session.add(new_tm_team)
            session.commit() # Commit, hogy kapjon ID-t
            team_id = new_tm_team.team_id
            logger.info(f"Új csapat felvéve (ID: {team_id}) a játékoshoz.")
        except Exception as e:
            session.rollback()
            logger.error(f"Hiba a játékoshoz felvett csapat mentésekor: {e}")
            return None
    return team_id


# --- TM API HÍVÁSOK ---
def fetch_tm_competition_data(comp_name):
    """
    Megkeresi a bajnokságot a Transfermarkt API-n név alapján.
    """
    try:
        url = f"{TM_API_URL}/competitions/search/{comp_name}"
        resp = requests_get_retry(url)
        
        if resp.status_code == 200:
            data = resp.json()
            if data.get('results'):
                result = data['results'][0] # Az első találatot elfogadjuk
                logger.info(f"TM Bajnokság találat: {result.get('name')} (ID: {result.get('id')})")
                return result
            else:
                logger.warning(f"TM API: Nem található bajnokság ezzel a névvel: {comp_name}")
    except Exception as e:
        logger.error(f"TM API Competition Search Error ({comp_name}): {e}")
    
    return None

def fetch_tm_player_search(tm_id, player_name):
    """
    Lekéri a játékos keresési eredményét Transfermarkt API-n
    """
    try:
        url = f"{TM_API_URL}/players/search/{player_name}"
        resp = requests_get_retry(url)
        if resp.status_code == 200:
            data = resp.json()
            if data.get('results'):
                for result in data['results']:
                    if result.get('id') == tm_id:
                        logger.info(f"TM Játékos találat: {result.get('name')} (ID: {result.get('id')})")
                        return result
                logger.warning(f"TM API: Kereséssel talált játékos ID-ja nem egyezik: {player_name} (ID: {tm_id})")
            logger.warning(f"TM API: Nem található játékos ezzel a névvel: {player_name}")
    except Exception as e:
        logger.error(f"TM API Player Search Error ({player_name}): {e}")
    return None

def fetch_tm_player_profile(tm_id):
    """
    Lekéri a játékost profilját Transfermarkt API-n
    """
    try:
        url = f"{TM_API_URL}/players/{tm_id}/profile"
        resp = requests_get_retry(url)
        if resp.status_code == 200:
            data = resp.json()
            return data
            
        logger.warning(f"Nem találtunk játékost (TM_ID: {tm_id})")
        return None

    except Exception as e:
        logger.error(f"TM API Player Profile Error (TM_ID: {tm_id}): {e}")
    return None

def fetch_tm_club_profile(tm_id):
    """
    Lekéri a játékost profilját Transfermarkt API-n
    """
    try:
        url = f"{TM_API_URL}/clubs/{tm_id}/profile"
        resp = requests_get_retry(url)
        if resp.status_code == 200:
            data = resp.json()
            return data
            
        logger.warning(f"Nem találtunk játékost (TM_ID: {tm_id})")
        return None

    except Exception as e:
        logger.error(f"TM API Club Profile Error (TM_ID: {tm_id}): {e}")
    return None

def fetch_tm_players_from_team(tm_team_id, season_year):
    """
    Lekéri egy csapat összes játékosát egy szezonon belül a Transfermarkt API-n
    """
    try:
        url = f"{TM_API_URL}/clubs/{tm_team_id}/players?season_id={season_year}"
        resp = requests_get_retry(url)
        if resp.status_code == 200:
            data = resp.json()
            return data.get('players', [])
        logger.warning(f"TM API: Nem található csapat keret ezzel az ID-val: {tm_team_id}")
    except Exception as e:
        logger.error(f"TM API Get Club Players Error ({tm_team_id}): {e}")
    return [] 

def fetch_tm_team_data_search(team_name, short_name):
    """
    Megkeresi a csapatot a Transfermarkt API-n név alapján.
    """
    try:
        url = f"{TM_API_URL}/clubs/search/{short_name}"
        resp = requests_get_retry(url)
        if resp.status_code == 200:
            data = resp.json()
            if data.get('results'):
                result = data['results'][0]  # Az első találatot elfogadjuk
                logger.info(f"TM Csapat találat: {result.get('name')} (ID: {result.get('id')})")
                return result 
            # Ha nincs talált próbáljuk meg teljes név alapján    
            else:
                url = f"{TM_API_URL}/clubs/search/{team_name}"
                resp = requests_get_retry(url)
                if resp.status_code == 200:
                    data = resp.json()
                    if data.get('results'):
                        result = data['results'][0]  # Az első találatot elfogadjuk
                        logger.info(f"TM Csapat találat teljes név alapján: {result.get('name')} (ID: {result.get('id')})")
                        return result 
                    else:
                        logger.warning(f"TM API: Nem található csapat ezzel a névvel: {team_name}")
    except Exception as e:
        logger.error(f"TM API Team Search Error ({team_name}): {e}")
    return None

def fetch_tm_market_value(tm_id):
    """Piaci érték történet lekérése."""
    try:
        url = f"{TM_API_URL}/players/{tm_id}/market_value"
        resp = requests_get_retry(url)
        if resp and resp.status_code == 200:
            return resp.json()
    except Exception as e:
        logger.error(f"TM API Market Value Error (TM_ID: {tm_id}): {e}")
    return None

def fetch_tm_transfers(tm_id):
    """Átigazolások lekérése."""
    try:
        url = f"{TM_API_URL}/players/{tm_id}/transfers"
        resp = requests_get_retry(url)
        if resp and resp.status_code == 200:
            return resp.json()
    except Exception as e:
        logger.error(f"TM API Transfers Error (TM_ID: {tm_id}): {e}")
    return None

def fetch_tm_stats(tm_id):
    """Statisztikák lekérése."""
    try:
        url = f"{TM_API_URL}/players/{tm_id}/stats"
        resp = requests_get_retry(url)
        if resp and resp.status_code == 200:
            return resp.json()
    except Exception as e:
        logger.error(f"TM API Stats Error (TM_ID: {tm_id}): {e}")
    return None