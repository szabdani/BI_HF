import time
import argparse
from datetime import datetime
from config import FD_API_KEY, TM_API_URL
from models import (
    DimTeam, FactMatch
)
from utils import (
    get_db_session, logger, FD_HEADERS, requests_get_retry, 
    get_or_create_season, get_or_create_competition, get_or_create_team, get_or_create_player,
    fetch_tm_competition_data, fetch_tm_player_search, fetch_tm_player_profile, 
    fetch_tm_club_profile, fetch_tm_players_from_team, fetch_tm_team_data_search
)

session = get_db_session()

# --- SEGÉDFÜGGVÉNYEK ---
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
            season_load_players_from_team(dim_team.tm_id, season_year)
        
        team_count += 1
        logger.info(f"Csapat és játékosai mentve és commitolva {team_count}/{len(teams_data)}")

def season_load_players_from_team(tm_team_id, season_year):
    """
    Lekéri és betölti egy csapat összes játékosát egy szezonon belül a DB-be.
    """
    players = fetch_tm_players_from_team(tm_team_id, season_year)
    for player_entry in players:
        get_or_create_player(player_entry['id'])


def season_load_matches(competition_obj, season_obj):
    """
    Lekéri és betölti egy szezon összes mérkőzését a DB-be.
    """
    url = f"http://api.football-data.org/v4/competitions/{competition_obj.fd_id}/matches?season={season_obj.start_year}"
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

    # Szezon létrehozása vagy lekérése
    season_obj = get_or_create_season(f"{season_year}/{season_year+1}", season_year, season_year+1)

    # Bajnokság lekérése a listából (FD API)
    competition_obj = season_load_competition(competition_code, season_year)
    
    # Összes csapat lekérése a listából (FD API)
    season_load_teams(competition_obj, season_year, with_players=True)

    # Összes meccs lekérése a listából (FD API)
    season_load_matches(competition_obj, season_obj)

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