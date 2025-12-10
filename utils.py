import time
import requests
import logging
from datetime import date, datetime
from sqlalchemy.orm import sessionmaker
from config import get_db_engine, FD_API_KEY, TM_API_URL

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