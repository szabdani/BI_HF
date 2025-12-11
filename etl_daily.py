import time
import argparse
from datetime import datetime, timedelta
from sqlalchemy.orm import sessionmaker
from config import FD_API_KEY
from models import (
    DimSeason, DimCompetition, DimTeam, DimPlayer, FactMatch, 
    FactMarketValue, FactTransfer, FactPlayerSeasonStat
)
from utils import (
    get_db_session, logger, requests_get_retry, FD_HEADERS,
    fetch_tm_club_profile, fetch_tm_market_value, fetch_tm_transfers, fetch_tm_stats, fetch_tm_players_from_team,
    get_or_create_player, get_season_from_TMname, get_or_create_team_by_tm_id, get_or_create_competition_by_tm_id
)

session = get_db_session()

# --- SEGÉDFÜGGVÉNYEK ---
def get_yesterday():
    """
    Visszaadja az előző nap dátumát YYYY-MM-DD string formátumban.
    """
    return (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

def get_current_season_tm_name():
    """
    Kiszámolja az aktuális szezont TM formátumban (pl. '23/24').
    Feltételezzük, hogy július 1-től új szezon van.
    """
    today = datetime.now()
    year = today.year
    if today.month < 7: # Ha év eleje van (pl. 2024 május), akkor a szezon 23/24
        return f"{str(year-1)[-2:]}/{str(year)[-2:]}"
    else: # Ha év vége (pl. 2024 augusztus), akkor 24/25
        return f"{str(year)[-2:]}/{str(year+1)[-2:]}"

def update_team_details(team):
    """
    Ellenőrzi és frissíti a csapat pénzügyi adatait (MarketValue, TransferRecord).
    """
    if not team.tm_id:
        return

    logger.info(f"Csapat adatainak ellenőrzése: {team.name}...")
    club_data = fetch_tm_club_profile(team.tm_id)
    if not club_data:
        return

    changed = False
    
    # Current Market Value ellenőrzés
    new_mv = club_data.get('currentMarketValue')
    if new_mv is not None and team.currentMarketValue != new_mv:
        logger.info(f"Csapat Market Value változott - {team.name}: {team.currentMarketValue} -> {new_mv}")
        team.currentMarketValue = new_mv
        changed = True

    # Transfer Record ellenőrzés
    new_tr = club_data.get('currentTransferRecord')
    if new_tr is not None and team.currentTransferRecord != new_tr:
        logger.info(f"Csapat Transfer Record változott - {team.name}: {team.currentTransferRecord} -> {new_tr}")
        team.currentTransferRecord = new_tr
        changed = True

    if changed:
        session.commit()
        logger.info(f"Csapat adatok frissítve - {team.name}")

def update_player_details(player, current_season_tm):
    """
    Frissíti a játékos csapatát, piaci értékét, átigazolásait és statisztikáit.
    """

    # Ha nincs TM ID, nem tudunk továbbmenni
    if not player.tm_id:
        return

    # Market Value ellenőrzés
    mv_data = fetch_tm_market_value(player.tm_id)
    if mv_data and 'marketValueHistory' in mv_data:
        # Megnézzük a legutolsó bejegyzést az API-ban
        latest_entry = mv_data['marketValueHistory'][-1]
        try:
            date_recorded = latest_entry.get('date')
            
            # Megnézzük, van-e már ilyen dátumú bejegyzésünk
            exists = session.query(FactTransfer).filter_by(
                player_id=player.player_id, 
                date_recorded=date_recorded
            ).first()

            if not exists:
                mv = FactMarketValue(
                    player_id=player.player_id,
                    date_recorded=date_recorded,
                    market_value_eur=latest_entry.get('marketValue'),
                    team_id=current_team_id
                )
                session.add(mv)
                session.commit()
                logger.info(f"Új Market Value rögzítve - {player.name}: (Régi: {player.current_team_id}, Új: {current_team_id})")
        except Exception as e:
            logger.error(f"Market Value Update Hiba: {e}")

    # Transfer History ellenőrzés (Új rekord)
    tf_data = fetch_tm_transfers(player.tm_id)
    if tf_data and 'transfers' in tf_data:
        # Megnézzük a legutolsó bejegyzést az API-ban
        latest_entry = tf_data['transfers'][-1]
        try:
            date_recorded = latest_entry.get('date')
            
            # Megnézzük, van-e már ilyen dátumú bejegyzésünk
            exists = session.query(FactMarketValue).filter_by(
                player_id=player.player_id, 
                date_recorded=date_recorded
            ).first()

            if not exists:
                # Transfer adatok lekérdezése
                season = get_season_from_TMname(latest_entry.get('season'))
                from_team_id = get_or_create_team_by_tm_id(latest_entry.get('clubFrom', {}).get('id'), latest_entry.get('clubFrom', {}).get('name'))
                to_team_id = get_or_create_team_by_tm_id(latest_entry.get('clubTo', {}).get('id'), latest_entry.get('clubTo', {}).get('name'))

                tf = FactTransfer(
                    player_id=player.player_id,
                    date_recorded=date_recorded,
                    teamFrom_id=from_team_id,
                    teamTo_id=to_team_id,
                    season_id=season.season_id,
                    market_value_eur=latest_entry.get('marketValue'),
                    fee_eur=latest_entry.get('fee')
                )

                logger.info(f"Átigazolás - {player.name} (Régi: {tf.teamFrom_id}, Új: {tf.teamTo_id})")
                player.current_team_id = tf.teamTo_id
                session.commit()
                
                session.add(mv)
                session.commit()
                logger.info(f"Új Transfer rögzítve - {player.name}: {date_recorded},  {latest_entry.get('marketValue')}")
        except Exception as e:
            logger.error(f"Transfer Update Hiba: {e}")


    # Statisztika Frissítése (CSAK PREMIER LEAGUE + IDEI SZEZON)
    stats_data = fetch_tm_stats(player.tm_id)
    
    if stats_data and 'stats' in stats_data:
        for entry in stats_data['stats']:
            # Szűrés: Szezon
            if entry.get('seasonID') != current_season_tm:
                continue
            
            # Szűrés: Bajnokság (Premier League)
            comp_tm_id = entry.get('competitionId')
            if comp_tm_id != "GB1":  # Premier League TM kódja
                continue

            # Megtaláltuk a PL idei statisztikáját. Keressük meg a DB-ben.
            # Először kell a szezon objektum ID-ja
            season_db = session.query(DimSeason).filter_by(season_name_TM=current_season_tm).first()
            if not season_db: continue

            competition = get_or_create_competition_by_tm_id(entry.get('competitionId'),entry.get('competitionName'))

            stat_record = session.query(FactPlayerSeasonStat).filter_by(
                player_id=player.player_id,
                season_id=season_db.season_id,
                competition_id=competition.competition_id
            ).first()

            # Adatok az API-ból
            api_apps = int(entry.get('appearances', 0))
            api_goals = int(entry.get('goals', 0))
            api_assists = int(entry.get('assists', 0))
            api_yellow_cards = int(entry.get('yellowCards', 0) or 0)
            api_red_cards = int(entry.get('redCards', 0) or 0)
            api_minutes = int(entry.get('minutesPlayed', 0) or 0)

            if stat_record:
                # Összehasonlítás: Ha változott, frissítjük
                if (stat_record.appearances != api_apps or 
                    stat_record.goals != api_goals or 
                    stat_record.assists != api_assists or
                    stat_record.minutes_played != api_minutes or
                    stat_record.yellow_cards != api_yellow_cards or
                    stat_record.red_cards != api_red_cards
                    ):
                    
                    logger.info(f"Statisztika frissítése: {player.name} (PL, {current_season_tm})")
                    stat_record.appearances = api_apps
                    stat_record.goals = api_goals
                    stat_record.assists = api_assists
                    stat_record.minutes_played = api_minutes
                    stat_record.yellow_cards = api_yellow_cards
                    stat_record.red_cards = api_red_cards
                    session.commit()
            else:
                # Ha még nincs rekord erre a szezonra, létrehozzuk
                new_stat = FactPlayerSeasonStat(
                    player_id=player.player_id,
                    team_id=current_team_id,
                    season_id=season_db.season_id,
                    competition_id=competition.competition_id,
                    appearances=api_apps,
                    goals=api_goals,
                    assists=api_assists,
                    minutes_played=api_minutes,
                    yellow_cards=api_yellow_cards,
                    red_cards=api_red_cards
                )
                session.add(new_stat)
                session.commit()
                logger.info(f"Új PL statisztika létrehozva: {player.name}")


# --- FŐ FÜGGVÉNY ---

def run_daily_etl():
    yesterday_str = get_yesterday()
    logger.info(f"--- NAPI ETL INDÍTÁSA: {yesterday_str} ---")
    
    current_season_tm = get_current_season_tm_name()
    logger.info(f"Aktuális szezon (TM): {current_season_tm}")

    # Csapatok frissítése
    teams_query = session.query(DimTeam).filter(DimTeam.tm_id.isnot(None))
    teams = teams_query.all()
    logger.info(f"Összesen {len(teams)} csapat részleteinek frissítése indul...")

    for i, team in enumerate(teams):
        logger.info(f"[{i+1}/{len(teams)}] Feldolgozás: {player.name}...")
        update_team_details(team)

    # Játékosok frissítése
    players_query = session.query(DimPlayer).filter(DimPlayer.tm_id.isnot(None))
    players = players_query.all()
    logger.info(f"Összesen {len(players)} játékos részleteinek frissítése indul...")

    for i, player in enumerate(players):
        logger.info(f"[{i+1}/{len(players)}] Feldolgozás: {player.name}...")
        update_player_details(player, current_season_tm)

    # Meccsek lekérése tegnapról (Football-Data API)
    # Csak PL (2021-es kód)
    COMPETITION_CODE = "PL" 
    
    url = f"http://api.football-data.org/v4/competitions/{COMPETITION_CODE}/matches?dateFrom={yesterday_str}&dateTo={yesterday_str}"
    resp = requests_get_retry(url, headers=FD_HEADERS)
    
    # Azon csapatok halmaza, akik játszottak tegnap
    teams_played_ids = set() 
    
    if resp and resp.status_code == 200:
        matches = resp.json().get('matches', [])
        logger.info(f"Tegnapi mérkőzések száma: {len(matches)}")

        season_obj = session.query(DimSeason).filter_by(season_name_TM=current_season_tm).first()
        competition_obj = session.query(DimCompetition).filter_by(fd_id="2021").first()  # PL fd_id = 2021
        
        for match_data in matches:
            if match_data['status'] == 'FINISHED':
                fd_match_id = match_data['id']
                
                # Ellenőrzés: megvan-e már?
                exists = session.query(FactMatch).filter_by(fd_match_id=fd_match_id).first()
                if not exists:
                    # Itt kéne a teljes FactMatch mentés logika (csapatok keresése, stb.)
                    # Mivel ez daily update, feltételezzük, hogy a csapatok már megvannak.
                    
                    # DB csapatok keresése FD ID alapján
                    home_team = session.query(DimTeam).filter_by(fd_id=match_data['homeTeam']['id']).first()
                    away_team = session.query(DimTeam).filter_by(fd_id=match_data['awayTeam']['id']).first()
                    
                    if home_team and away_team:
                        # Hozzáadjuk őket a frissítendő listához
                        teams_played_ids.add(home_team.team_id)
                        teams_played_ids.add(away_team.team_id)
                        
                        # Match mentése
                        match_fact = FactMatch(
                            fd_match_id=fd_match_id,
                            date=datetime.strptime(match_data['utcDate'], "%Y-%m-%dT%H:%M:%SZ"),
                            season_id=season_obj.season_id,
                            competition_id=competition_obj.competition_id,
                            home_team_id=home_team.team_id,
                            away_team_id=away_team.team_id,
                            home_score=match_data['score']['fullTime']['home'],
                            away_score=match_data['score']['fullTime']['away'],
                            status=match_data['status']
                        )
                        logger.info(f"Meccs feldolgozva: {home_team.name} vs {away_team.name}")
                    else:
                        logger.warning(f"Ismeretlen csapatok a meccsben: {fd_match_id}")
    else:
        logger.error("Nem sikerült lekérni a tegnapi meccseket.")
        return

    if not teams_played_ids:
        logger.info("Nem volt tegnap releváns mérkőzés. Leállítás.")
        return

    logger.info("Napi ETL sikeresen befejeződött.")

if __name__ == "__main__":
    try:
        run_daily_etl()
    except Exception as e:
        logger.error(f"Hiba a napi ETL során: {e}")