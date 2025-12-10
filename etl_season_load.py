import time
import requests
import logging
from datetime import date
from datetime import datetime
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from config import get_db_engine, FD_API_KEY, TM_API_URL
from models import (
    Base, DimSeason, DimCompetition, DimTeam, DimPlayer, 
    FactMatch, FactPlayerPerformance
)

# --- KONFIGURÁCIÓ ÉS LOGOLÁS ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

engine = get_db_engine()
Session = sessionmaker(bind=engine)
session = Session()

FD_HEADERS = {'X-Auth-Token': FD_API_KEY}
RATE_LIMIT_SLEEP = 7  # 10 hívás / perc -> 6 másodpercenként 1 hívás
TM_SLEEP_BETWEEN_CALLS = 3 # TM API hívások között

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
        time.sleep(TM_SLEEP_BETWEEN_CALLS)
        tm_data = fetch_tm_competition_data(name)
        if tm_data:
            comp.tm_id = tm_data.get('id')
            comp.country = tm_data.get('country')
            comp.continent = tm_data.get('continent')

        session.add(comp)
        session.commit()
        logger.info(f"Bajnokság elmentve: {name} (TM ID: {comp.tm_id})")
        
    return comp

def calculate_age(born):
    """
    Kiszámolja az életkort a születési dátum alapján a mai napra.
    """
    if not born:
        return None
    today = date.today()
    return today.year - born.year - ((today.month, today.day) < (born.month, born.day))

def fetch_tm_player_data(player_name, fd_dob_date):
    """
    Megkeresi a játékost a Transfermarkt API-n név alapján: 
    DE életkort is ellenőriz.
    """
    try:
        url = f"{TM_API_URL}/players/search/{player_name}"
        resp = requests_get_retry(url)
        if resp.status_code == 200:
            data = resp.json()
            results = data.get('results', [])
            if not results:
                return None

            # Ha nincs születési dátumunk az FD-ből, elfogadjuk az elsőt
            if not fd_dob_date:
                logger.warning(f"Nincs FD születési dátum: {player_name}, az első találatot választjuk.")
                return results[0]

            # Életkor számítása
            calculated_age = calculate_age(fd_dob_date)
            
            # Végigmegyünk a találatokon és keressük az egyezést
            for res in results:
                tm_age_str = res.get('age')
                if tm_age_str and tm_age_str.isdigit():
                    tm_age = int(tm_age_str)
                    
                    # +- 1 év eltérést engedünk 
                    if abs(tm_age - calculated_age) <= 1:
                        logger.info(f"Sikeres párosítás: {player_name} (FD Age: {calculated_age}, TM Age: {tm_age})")
                        return res
            
            logger.warning(f"Nem találtunk életkorban egyező játékost: {player_name} (Keresett kor: {calculated_age})")
            return None

    except Exception as e:
        logger.error(f"TM API Player Search Error ({player_name}): {e}")
    return None

def get_or_create_player(player_entry, team_id):
    """
    Megkeresi a játékost a DB-ben, ha nincs készít.
    """
    fd_id = player_entry['id']
    player = session.query(DimPlayer).filter_by(fd_id=fd_id).first()
    
    if not player:
        logger.info(f"Új játékos feldolgozása: {player_entry['name']}...")    

        time.sleep(RATE_LIMIT_SLEEP)
        url = f"http://api.football-data.org/v4/persons/{fd_id}"
        resp = requests_get_retry(url, headers=FD_HEADERS)
        if not resp or resp.status_code != 200:
            logger.error(f"Hiba a játékos lekérdezésénél: {resp.status_code}")
            return None
        
        player_data = resp.json()

        # Dátum konverzió
        dob_date = None
        if player_data.get('dateOfBirth'):
            try:
                dob_date = datetime.strptime(player_data['dateOfBirth'], '%Y-%m-%d').date()
            except: pass

        # TM Adatok keresése a név ÉS a születési dátum átadásával
        time.sleep(TM_SLEEP_BETWEEN_CALLS)
        tm_data = fetch_tm_player_data(player_data['name'], dob_date)
        
        player = DimPlayer(
            fd_id=fd_id,
            name=player_data['name'],
            position=player_data.get('position'),
            date_of_birth=player_data.get('dateOfBirth'),
            nationality=player_data.get('nationality'),
            shirt_number=player_data.get('shirtNumber'),
            current_team_id=team_id
        )
        
        if tm_data:
            player.tm_id = tm_data.get('id')
        
        session.add(player)
        session.flush() 
    
    return player

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
        time.sleep(TM_SLEEP_BETWEEN_CALLS)
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
            time.sleep(TM_SLEEP_BETWEEN_CALLS) 

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
        
        # Játékosok (Lineup + Bench) feldolgozása ÉS Teljesítmény gyűjtés
        # Egy szótárba gyűjtjük a játékosok teljesítményét {fd_player_id: PerformanceObj}
        performances = {}

        def process_squad_list(squad_list, team_db_id, side_name):
            """Segédfüggvény a kezdő és csere feldolgozására"""
            for player_entry in squad_list:
                # Játékos létrehozása/lekérése
                p_obj = get_or_create_player(player_entry, team_db_id)
                
                # Performance init (alapértelmezett értékekkel)
                perf = FactPlayerPerformance(
                    player_id=p_obj.player_id,
                    team_id=team_db_id,
                    minutes_played=0,
                    goals=0,
                    assists=0,
                    yellow_cards=0,
                    red_cards=0
                )
                performances[p_obj.fd_id] = perf

        # Hazai és vendég játékosok
        process_squad_list(match_detail['homeTeam'].get('lineup', []), home_team.team_id, match_detail['homeTeam']['name'])
        process_squad_list(match_detail['homeTeam'].get('bench', []), home_team.team_id, match_detail['homeTeam']['name'])
        process_squad_list(match_detail['awayTeam'].get('lineup', []), away_team.team_id, match_detail['awayTeam']['name'])
        process_squad_list(match_detail['awayTeam'].get('bench', []), away_team.team_id, match_detail['awayTeam']['name'])

        # Események feldolgozása -> Performance objektumok frissítése
        # Gólok
        for goal in match_detail.get('goals', []):
            scorer_id = goal['scorer']['id']
            assist_id = goal['assist']['id']
            
            if scorer_id and scorer_id in performances:
                performances[scorer_id].goals += 1
            if assist_id and assist_id in performances:
                performances[scorer_id].assists += 1

        # Lapok
        for booking in match_detail.get('bookings', []):
            card_player_id = booking['player']['id']
            card_type = booking['card']
            
            if card_player_id in performances:
                if card_type == 'YELLOW':
                    performances[card_player_id].yellow_cards += 1
                elif card_type == 'RED':
                    performances[card_player_id].red_cards += 1

        # Percek számítása (Kezdő: full duration, Csere: +- mikor cserélték)
        # Ha nincs injuryTime, legyen 0, ha nincs minute, legyen 90
        injury_time = match_detail.get('injuryTime') if match_detail.get('injuryTime') else 0
        full_time = match_detail.get('minute') if match_detail.get('minute') else 90
        match_duration = full_time + injury_time
        
        # Kezdők
        for p_entry in match_detail['homeTeam'].get('lineup', []) + match_detail['awayTeam'].get('lineup', []):
            if p_entry['id'] in performances:
                 performances[p_entry['id']].minutes_played = match_duration

        # Cserék
        for sub in match_detail.get('substitutions', []):
            player_out_id = sub['playerOut']['id']
            player_in_id = sub['playerIn']['id']
            minute = sub['minute']
            
            # Aki lejött: mennyi ideig játszott
            if player_out_id in performances:
                performances[player_out_id].minutes_played = minute
            
            # Aki bejött: full - mikor jött be
            if player_in_id in performances:
                performances[player_in_id].minutes_played = match_duration - minute

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
        session.flush() # Ki kell nyerni a match_id-t

        # Performance mentése akik játszottak, vagy kaptak lapot
        count_saved = 0
        for perf in performances.values():
            if perf.minutes_played > 0 or perf.yellow_cards > 0 or perf.red_cards > 0:
                perf.match_id = match_fact.match_id
                session.add(perf)
                count_saved += 1

        session.commit()
        logger.info(f"Meccs mentve és commitolva: {fd_match_id}")

    logger.info("A teljes szezon feldolgozása befejeződött.")

if __name__ == "__main__":
    # Futtatás a Premier League (PL) 2023-as szezonjára (ami a 23/24-es szezon)
    try:
        run_season_load(competition_code="PL", season_year=2023)
    except KeyboardInterrupt:
        print("Leállítás...")
    except Exception as e:
        logger.error(f"Végzetes hiba: {e}")