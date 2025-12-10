import time
import argparse
from datetime import datetime
from config import FD_API_KEY, TM_API_URL
from models import (
    Base, DimSeason, DimCompetition, DimTeam, DimPlayer, FactMatch
)
from utils import (
    get_db_session, logger, FD_HEADERS, requests_get_retry, fetch_tm_competition_data, 
    fetch_tm_player_search, fetch_tm_player_profile, fetch_tm_club_profile, fetch_tm_players_from_team,
    fetch_tm_team_data_search
)

session = get_db_session()

# --- SEGÉDFÜGGVÉNYEK ---
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

def get_or_create_player(tm_id, competition_id):
    """
    Megkeresi a játékost a DB-ben, ha nincs készít.
    """
    player = session.query(DimPlayer).filter_by(tm_id=tm_id).first()

    # Diogo Jota elhunyt játékos, akire nem működik a TM API profile hívás
    if(tm_id == "340950"):    
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
                    competition_id=None 
                )
                 
                # Megpróbáljuk a részleteket lekérni
                club_data = fetch_tm_club_profile(tm_club_id)
                if club_data:
                    new_tm_team.founded = club_data.get('foundedOn')
                    new_tm_team.stadium = club_data.get('stadiumName')
                    new_tm_team.currentTransferRecord = club_data.get('currentTransferRecord')
                    new_tm_team.currentMarketValue = club_data.get('currentMarketValue')

                try:
                    session.add(new_tm_team)
                    session.commit() # Commit, hogy kapjon ID-t
                    internal_team_id = new_tm_team.team_id
                    logger.info(f"Új csapat felvéve (ID: {internal_team_id}) a játékoshoz.")
                except Exception as e:
                    session.rollback()
                    logger.error(f"Hiba a játékoshoz felvett csapat mentésekor: {e}")
                    return None

        player = DimPlayer(
            name=player_data['name'],
            tm_id=tm_id,
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

def season_load_competition(competition_code, season_year):
    """
    Lekéri és betölti egy bajnokság adatait a DB-be.
    """
    # Bajnokság lekérése a listából (FD API)
    url = f"http://api.football-data.org/v4/competitions/{competition_code}"
    resp = requests_get_retry(url, headers=FD_HEADERS)
    if resp.status_code != 200:
        logger.error(f"Hiba a meccsek listázásánál: {resp.status_code}")
        return None
    
    comp_meta = resp.json()
    competition_obj = get_or_create_competition(comp_meta.get('code'), comp_meta.get('name'), comp_meta.get('emblem'))
    
    return competition_obj

def season_load_teams(competition_obj, season_year, with_players=False):
    """
    Lekéri és betölti egy szezon összes csapatát a DB-be.
    """
     # Összes csapat lekérése a listából (FD API)
    url = f"http://api.football-data.org/v4/competitions/{competition_obj.fd_id}/teams?season={season_year}"
    resp = requests_get_retry(url, headers=FD_HEADERS)
    if resp.status_code != 200:
        logger.error(f"Hiba a meccsek listázásánál: {resp.status_code}")
        return
    
    teams_data = resp.json().get('teams', [])
    logger.info(f"Összesen {len(teams_data)} csapat talált.")
    team_count = 0
    for team in teams_data:
        # Csapatok feldolgozása
        dim_team = get_or_create_team(team, competition_obj.competition_id)

        if with_players:
            season_load_players_from_team(dim_team.tm_id, season_year, competition_obj.competition_id)
        
        team_count += 1
        logger.info(f"Csapat és játékosai mentve és commitolva {team_count}/{len(teams_data)}")

def season_load_players_from_team(tm_team_id, season_year, competition_id):
    """
    Lekéri és betölti egy csapat összes játékosát egy szezonon belül a DB-be.
    """
    players = fetch_tm_players_from_team(tm_team_id, season_year)
    for player_entry in players:
        get_or_create_player(player_entry['id'], competition_id)


def season_load_matches(competition_obj, season_year):
    """
    Lekéri és betölti egy szezon összes mérkőzését a DB-be.
    """
    url = f"http://api.football-data.org/v4/competitions/{competition_obj.fd_id}/matches?season={season_year}"
    resp = requests_get_retry(url, headers=FD_HEADERS)
    if resp.status_code != 200:
        logger.error(f"Hiba a meccsek listázásánál: {resp.status_code}")
        return

    matches_data = resp.json().get('matches', [])

    # Iterálás a meccseken
    match_count = 0
    for match in matches_data:
        fd_match_id = match['id']
        logger.info(f"Meccs feldolgozása: {fd_match_id} ({match['homeTeam']['name']} vs {match['awayTeam']['name']})")
        match_count += 1

        # Ellenőrizzük, hogy megvan-e már
        existing_match = session.query(FactMatch).filter_by(fd_match_id=fd_match_id).first()
        if existing_match and existing_match.status == 'FINISHED':
            logger.info(f"Meccs már feldolgozva: {fd_match_id}, ugrás.")
            continue
        
        if match['status'] != 'FINISHED':
            continue

        home_team = session.query(DimTeam).filter_by(fd_id=match['homeTeam']['id']).first()
        away_team = session.query(DimTeam).filter_by(fd_id=match['awayTeam']['id']).first()

        # Match mentése
        match_fact = FactMatch(
            fd_match_id=fd_match_id,
            date=datetime.strptime(match['utcDate'], "%Y-%m-%dT%H:%M:%SZ"),
            season_id=season_obj.season_id,
            competition_id=competition_obj.competition_id,
            home_team_id=home_team.team_id,
            away_team_id=away_team.team_id,
            home_score=match['score']['fullTime']['home'],
            away_score=match['score']['fullTime']['away'],
            status=match['status']
        )
        session.add(match_fact)
        session.commit()
        logger.info(f"Meccs mentve és commitolva {match_count}/{len(matches_data)}: {fd_match_id}")

# --- FŐ FÜGGVÉNY ---

def run_season_load(competition_code="PL", season_year=2024):
    """
    A fő függvény, ami végigmegy a szezon összes meccsén.
    """
    session.rollback()
    logger.info(f"--- Season load indítása: {competition_code} {season_year} ---")

    season_obj = get_or_create_season(f"{season_year}/{season_year+1}", season_year, season_year+1)

    # Bajnokság lekérése a listából (FD API)
    competition_obj = season_load_competition(competition_code, season_year)
    
    # Összes csapat lekérése a listából (FD API)
    season_load_teams(competition_obj, season_year, with_players=True)

    # Összes meccs lekérése a listából (FD API)
    season_load_matches(competition_obj, season_year)

    logger.info("A teljes szezon feldolgozása befejeződött.")

# --- FŐ FÜGGVÉNY FUTTATÁSA ---

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Foci adat ETL job indítása Football-Data és Transfermarkt API-król.",
        formatter_class=argparse.RawTextHelpFormatter
    )
    
    parser.add_argument(
        '-c', '--competition', 
        type=str, 
        default='PL', 
        help="Football-Data bajnokság kódja (pl. PL, BL1, SA). Alapértelmezett: PL."
    )
    
    parser.add_argument(
        '-y', '--year', 
        type=int, 
        default=2023, 
        help="A szezon kezdő éve (pl. 2023 a 2023/2024 szezonhoz). Alapértelmezett: 2023."
    )

    args = parser.parse_args()

    # Ellenőrizzük, hogy a bemeneti év reális-e
    current_year = datetime.now().year
    if args.year > current_year:
        logger.error(f"Hiba: A megadott év ({args.year}) a jövőben van. A maximálisan megengedett év: {current_year}.")
        exit(1)
        
    try:
        run_season_load(competition_code=args.competition, season_year=args.year)
    except KeyboardInterrupt:
        print("\nLeállítás a felhasználó által (Ctrl+C).")
    except Exception as e:
        logger.error(f"Végzetes hiba történt a(z) {args.competition} {args.year}/{args.year+1} szezon töltésekor:")
        logger.error(e)