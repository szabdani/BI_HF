# etl_player_data.py
import time
import argparse
from datetime import datetime
from models import (
    DimPlayer, FactMarketValue, FactTransfer, FactPlayerSeasonStat
)
from utils import (
    get_db_session, logger, fetch_tm_market_value, fetch_tm_transfers, fetch_tm_stats,
    get_season_from_TMname, get_or_create_team_by_tm_id, get_or_create_competition_by_tm_id
)

session = get_db_session()

def process_player_market_values(player):
    """
    Feldolgozza és menti a piaci érték történetet.
    """
    logger.info(f"Market Values lekérése: {player.name} (TM ID: {player.tm_id})")
    
    data = fetch_tm_market_value(player.tm_id)
    if not data or 'marketValueHistory' not in data:
        return

    count = 0
    for entry in data['marketValueHistory']:
        # Duplikáció ellenőrzése
        exists = session.query(FactMarketValue).filter_by(
            player_id=player.player_id, 
            date_recorded=entry.get('date')
        ).first()
        
        if not exists:
            # Csapat keresése (TM ID alapján)
            team_id = get_or_create_team_by_tm_id(entry.get('clubId'), entry.get('clubName'))
            mv = FactMarketValue(
                player_id=player.player_id,
                date_recorded=entry.get('date'),
                market_value_eur=entry.get('marketValue'),
                team_id=team_id
            )
            session.add(mv)
            count += 1
            
    session.commit()
    logger.info(f"{player.name} (ID: {player.player_id}) {count} új piaci érték bejegyzés mentve.")

def process_player_transfers(player):
    """
    Feldolgozza és menti az átigazolásokat.
    """
    logger.info(f"Transfers lekérése: {player.name}")
    
    data = fetch_tm_transfers(player.tm_id)
    if not data or 'transfers' not in data:
        return

    count = 0
    for entry in data['transfers']:
        # Duplikáció ellenőrzése
        exists = session.query(FactTransfer).filter_by(
            player_id=player.player_id,
            date_recorded=entry.get('date')
        ).first()

        if not exists:
            season = get_season_from_TMname(entry.get('season'))

            from_team_id = get_or_create_team_by_tm_id(entry.get('clubFrom', {}).get('id'), entry.get('clubFrom', {}).get('name'))
            to_team_id = get_or_create_team_by_tm_id(entry.get('clubTo', {}).get('id'), entry.get('clubTo', {}).get('name'))

            tf = FactTransfer(
                player_id=player.player_id,
                date_recorded=entry.get('date'),
                teamFrom_id=from_team_id,
                teamTo_id=to_team_id,
                season_id=season.season_id,
                market_value_eur=entry.get('marketValue'),
                fee_eur=entry.get('fee')
            )
            session.add(tf)
            count += 1

    session.commit()
    logger.info(f"{player.name} (ID: {player.player_id}) {count} új átigazolás mentve.")

def process_player_season_stats(player):
    """
    Szezonális statisztikák betöltése.
    """
    logger.info(f"Season stats lekérése: {player.name} (TM ID: {player.tm_id})")
    
    data = fetch_tm_stats(player.tm_id)
    if not data or 'stats' not in data:
        return

    count = 0
    for entry in data['stats']:
        # Szezon és bajnokság lekérése
        season = get_season_from_TMname(entry.get('seasonId'))
        comp = get_or_create_competition_by_tm_id(entry.get('competitionId'),entry.get('competitionName'))
        
        # Duplikáció ellenőrzése
        exists = session.query(FactPlayerSeasonStat).filter_by(
            player_id=player.player_id, 
            season_id=season.season_id,
            competition_id=comp.competition_id
        ).first()
        
        if not exists:
            # Csapat keresése (TM ID alapján)
            team_id = get_or_create_team_by_tm_id(entry.get('clubId'), None)
            mv = FactPlayerSeasonStat(
                player_id=player.player_id,
                team_id=team_id,
                season_id=season.season_id,
                competition_id=comp.competition_id,
                appearances=entry.get('appearances'),
                goals=entry.get('goals'),
                assists=entry.get('assists'),
                yellow_cards=entry.get('yellowCards'),
                red_cards=entry.get('redCards'),
                minutes_played=entry.get('minutesPlayed')
            )
            session.add(mv)
            count += 1
            
    session.commit()
    logger.info(f"{player.name} (ID: {player.player_id}) {count} új szezon bajnoksági statisztika mentve.")

def run_player_details_etl(limit=None):
    """
    Fő ciklus: Végigmegy a DimPlayer táblán és frissíti a részleteket.
    """
    players_query = session.query(DimPlayer).filter(DimPlayer.tm_id.isnot(None))
    
    if limit:
        players_query = players_query.limit(limit)
        
    players = players_query.all()
    logger.info(f"Összesen {len(players)} játékos részleteinek frissítése indul...")

    for i, player in enumerate(players):
        logger.info(f"[{i+1}/{len(players)}] Feldolgozás: {player.name}...")
        
        process_player_market_values(player)
        process_player_transfers(player)
        process_player_season_stats(player)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Játékos részletek (Market Value, Transfer, Stats) betöltése.")
    parser.add_argument('-l', '--limit', type=int, help="Limit a teszteléshez (pl. 5 játékos).")
    args = parser.parse_args()

    try:
        run_player_details_etl(limit=args.limit)
    except KeyboardInterrupt:
        print("\nLeállítás...")