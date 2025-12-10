import time
import requests
import logging
from datetime import date
from datetime import datetime
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from config import get_db_engine, FD_API_KEY, TM_API_URL
from models import (
    Base, DimSeason, DimCompetition, DimTeam, DimPlayer, FactMatch
)

# --- KONFIGURÁCIÓ ÉS LOGOLÁS ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

engine = get_db_engine()
Session = sessionmaker(bind=engine)
session = Session()

FD_HEADERS = {'X-Auth-Token': FD_API_KEY}
RATE_LIMIT_SLEEP = 7  # 10 hívás / perc -> 6 másodpercenként 1 hívás

# --- SEGÉDFÜGGVÉNYEK ---
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

def get_or_create_competition(fd_code, name, emblem_url):
    """
    Megkeresi a bajnokságot a DB-ben, ha nincs készít.
    """
    comp = session.query(DimCompetition).filter_by(fd_id=fd_code).first()
    
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
            comp.tm_id = tm_data.get('id')
            comp.country = tm_data.get('country')
            comp.continent = tm_data.get('continent')

        session.add(comp)
        session.commit()
        logger.info(f"Bajnokság elmentve: {name} (TM ID: {comp.tm_id})")
        
    return comp

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
            
        logger.warning(f"Nem találtunk életkorban egyező játékost: {player_name} (Keresett kor: {calculated_age})")
        return None

    except Exception as e:
        logger.error(f"TM API Player Profile Error ({player_name}): {e}")
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
 

def get_or_create_player(tm_id, competition_id):
    """
    Megkeresi a játékost a DB-ben, ha nincs készít.
    """
    player = session.query(DimPlayer).filter_by(tm_id=tm_id).first()
    
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
        tm_club_id = player_search_data.get('club').get('id') if player_search_data and player_search_data.get('club') else None
        if tm_club_id:
            team_mapping = session.query(DimTeam).filter_by(tm_id=tm_club_id).first()
            internal_team_id = None
            if team_mapping:
                internal_team_id = team_mapping.team_id 
            else:
                # Ha nincs a DB-ben, létrehozzuk a csapatot csak TM adatokból
                tm_club_name = player_search_data.get('club').get('name')
                logger.info(f"Hiányzó csapat ({tm_club_name}, ID: {tm_club_id}) létrehozása...")
                 
                new_tm_team = DimTeam(
                    fd_id=None, # Nincs FD ID
                    tm_id=tm_club_id,
                    name=tm_club_name,
                    # A többi adatot megpróbáljuk lekérni, de ha nem sikerül, marad NULL
                    competition_id=None 
                )
                 
                # Megpróbáljuk a részleteket lekérni
                try:
                    url = f"{TM_API_URL}/clubs/{tm_club_id}/profile"
                    resp = requests_get_retry(url)
                    if resp and resp.status_code == 200:
                        club_data = resp.json()
                        new_tm_team.founded = club_data.get('foundedOn')
                        new_tm_team.stadium = club_data.get('stadiumName')
                        new_tm_team.currentTransferRecord = club_data.get('currentTransferRecord')
                        new_tm_team.currentMarketValue = club_data.get('currentMarketValue')
                except Exception:
                    logger.warning(f"Nem sikerült lekérni a csapat profilját TM ID: {tm_club_id}")
                    pass # Ha nem sikerül a profil, nem baj, a név és ID megvan!

                try:
                    session.add(new_tm_team)
                    session.commit() # Commit, hogy kapjon ID-t
                    internal_team_id = new_tm_team.team_id
                    logger.info(f"Új csapat felvéve (ID: {internal_team_id}) a játékoshoz.")
                except Exception as e:
                    session.rollback()
                    logger.error(f"Hiba a játékoshoz felvett csapat mentésekor: {e}")
                    internal_team_id = None

        player = DimPlayer(
            name=player_data['name'],
            position=player_search_data.get('position'),  
            position_name=player_data.get('position').get('main'),
            nationality=main_nationality,
            age=player_search_data.get('age'),    
            shirt_number=player_data.get('shirtNumber'),
            current_team_id=internal_team_id,
        )

        if internal_team_id is None:
            logger.warning(f"Játékos {player_data['name']} mentése csapat-hivatkozás nélkül.")
        
        session.add(player)
        session.flush() 
    
    return player

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

def fetch_tm_team_data(team_name, short_name):
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

def get_or_create_team(fd_team_data, competition_id):
    """
    Ellenőrzi, hogy a csapat létezik-e. Ha nem, létrehozza FD + TM adatokból.
    """
    fd_id = fd_team_data['id']
    team = session.query(DimTeam).filter_by(fd_id=fd_id).first()
    
    if not team:
        logger.info(f"Új csapat feldolgozása: {fd_team_data['name']}...")
        
        # TM Adatok lekérése
        tm_data = fetch_tm_team_data(fd_team_data['name'], fd_team_data.get('shortName'))
        
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

            # TM Csapat profil lekérése a hiányzó adatokért
            try:
                url = f"{TM_API_URL}/clubs/{team.tm_id}/profile"
                resp = requests_get_retry(url)
                if resp.status_code == 200:
                    data = resp.json()
                    logger.info(f"TM Csapat profil találat: {data.get('name')} (ID: {data.get('id')})")
                    team.founded = data.get('foundedOn')
                    team.stadium = data.get('stadiumName')
                    team.currentTransferRecord = data.get('currentTransferRecord')
                    team.currentMarketValue = data.get('currentMarketValue') 
                else:
                    logger.warning(f"TM API: Nem található csapat profil ezzel az ID-val: {team.tm_id}")
            except Exception as e:
                logger.error(f"TM API Get Club Profile Error ({team.tm_id}): {e}")
            
        session.add(team)
        session.commit()
    
    return team

# --- FŐ FÜGGVÉNY ---

def run_season_load(competition_code="PL", season_year=2024):
    """
    A fő függvény, ami végigmegy a szezon összes meccsén.
    """
    session.rollback()
    logger.info(f"--- Season load indítása: {competition_code} {season_year} ---")

    season_obj = get_or_create_season(f"{season_year}/{season_year+1}", season_year, season_year+1)
    
    # Összes meccs lekérése a listából (FD API)
    url = f"http://api.football-data.org/v4/competitions/{competition_code}/matches?season={season_year}"
    resp = requests_get_retry(url, headers=FD_HEADERS)
    if resp.status_code != 200:
        logger.error(f"Hiba a meccsek listázásánál: {resp.status_code}")
        return

    matches_data = resp.json().get('matches', [])
    comp_meta = resp.json().get('competition', {})
    competition_obj = get_or_create_competition(comp_meta.get('code'), comp_meta.get('name'), comp_meta.get('emblem'))
    
    logger.info(f"Összesen {len(matches_data)} mérkőzés talált.")

    # Iterálás a meccseken
    match_count = 0
    for match_basic in matches_data:
        fd_match_id = match_basic['id']
        
        # Ellenőrizzük, hogy megvan-e már
        existing_match = session.query(FactMatch).filter_by(fd_match_id=fd_match_id).first()
        if existing_match and existing_match.status == 'FINISHED':
            logger.info(f"Meccs már feldolgozva: {fd_match_id}, ugrás.")
            continue
        
        if match_basic['status'] != 'FINISHED':
            continue

        time.sleep(RATE_LIMIT_SLEEP)
        logger.info(f"Meccs részletek lekérése: {fd_match_id} ({match_basic['homeTeam']['name']} vs {match_basic['awayTeam']['name']})")
        
        detail_url = f"http://api.football-data.org/v4/matches/{fd_match_id}"
        detail_resp = requests_get_retry(detail_url, headers=FD_HEADERS)
        if detail_resp.status_code != 200:
            logger.error(f"Hiba a meccs részleteknél ({fd_match_id}): {detail_resp.status_code}")
            continue
            
        match_detail = detail_resp.json()
        
        # Csapatok feldolgozása
        home_team = get_or_create_team(match_detail['homeTeam'], competition_obj.competition_id)
        away_team = get_or_create_team(match_detail['awayTeam'], competition_obj.competition_id)
    
        # Játékosok feldolgozása
        home_players = fetch_tm_players_from_team(home_team.tm_id, season_year)
        for player_entry in home_players:
            get_or_create_player(player_entry['id'], competition_obj.competition_id)

        away_players = fetch_tm_players_from_team(away_team.tm_id, season_year)
        for player_entry in away_players:
            get_or_create_player(player_entry['id'], competition_obj.competition_id)

        # Match mentése
        match_fact = FactMatch(
            fd_match_id=fd_match_id,
            date=datetime.strptime(match_detail['utcDate'], "%Y-%m-%dT%H:%M:%SZ"),
            season_id=season_obj.season_id,
            competition_id=competition_obj.competition_id,
            home_team_id=home_team.team_id,
            away_team_id=away_team.team_id,
            home_score=match_detail['score']['fullTime']['home'],
            away_score=match_detail['score']['fullTime']['away'],
            status=match_detail['status']
        )
        session.add(match_fact)
        session.commit()
        match_count += 1
        logger.info(f"Meccs mentve és commitolva {match_count}/{len(matches_data)}: {fd_match_id}")

    logger.info("A teljes szezon feldolgozása befejeződött.")

if __name__ == "__main__":
    # Futtatás a Premier League (PL) 2023-as szezonjára (ami a 23/24-es szezon)
    try:
        run_season_load(competition_code="PL", season_year=2023)
    except KeyboardInterrupt:
        print("Leállítás...")
    except Exception as e:
        logger.error(f"Végzetes hiba: {e}")