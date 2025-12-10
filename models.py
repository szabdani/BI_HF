from sqlalchemy import Column, Integer, String, Date, ForeignKey, Float, DateTime, Boolean
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()

# --- DIMENZIÓ TÁBLÁK ---

class DimSeason(Base):
    __tablename__ = 'dim_seasons'
    season_id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, unique=True)
    season_name_TM = Column(String, unique=True) 
    start_year = Column(Integer)
    end_year = Column(Integer)

class DimCompetition(Base):
    __tablename__ = 'dim_competitions'
    competition_id = Column(Integer, primary_key=True, autoincrement=True)
    fd_id = Column(String, unique=True, nullable=True)
    tm_id = Column(String, unique=True, nullable=True)

    name = Column(String)
    emblem_url = Column(String)
    country = Column(String)
    continent = Column(String)

class DimTeam(Base):
    __tablename__ = 'dim_teams'
    team_id = Column(Integer, primary_key=True, autoincrement=True)
    fd_id = Column(Integer, unique=True, nullable=True)
    tm_id = Column(Integer, unique=True, nullable=True)
    
    name = Column(String)
    short_name = Column(String)
    tla = Column(String(3)) # Pl: MUN, CHE
    crest_url = Column(String)
    founded = Column(Date)
    stadium = Column(String)
    currentTransferRecord = Column(Integer)
    currentMarketValue = Column(Integer)

    competition_id = Column(Integer, ForeignKey('dim_competitions.competition_id'))
    competition = relationship("DimCompetition", backref="teams")

class DimPlayer(Base):
    __tablename__ = 'dim_players'
    player_id = Column(Integer, primary_key=True, autoincrement=True)
    tm_id = Column(Integer, unique=True, nullable=True)
    
    name = Column(String)
    position = Column(String)
    position_name = Column(String)
    nationality = Column(String)
    age = Column(Integer, nullable=True)
    shirt_number = Column(String, nullable=True)

    current_team_id = Column(Integer, ForeignKey('dim_teams.team_id'), nullable=True)
    current_team = relationship("DimTeam", backref="current_players")

# --- TÉNY TÁBLÁK ---

class FactMatch(Base):
    """
    A mérkőzések végeredményét tárolja. 
    Ebből számoljuk a tabellát (W-D-L, Pontok).
    """
    __tablename__ = 'fact_matches'
    match_id = Column(Integer, primary_key=True, autoincrement=True)
    fd_match_id = Column(Integer, unique=True)
    date = Column(DateTime)
    
    season_id = Column(Integer, ForeignKey('dim_seasons.season_id'))
    competition_id = Column(Integer, ForeignKey('dim_competitions.competition_id'))
    
    home_team_id = Column(Integer, ForeignKey('dim_teams.team_id'))
    away_team_id = Column(Integer, ForeignKey('dim_teams.team_id'))
    
    home_score = Column(Integer)
    away_score = Column(Integer)
    status = Column(String) # 'FINISHED', 'SCHEDULED'


class FactMarketValue(Base):
    """
    A játékos piaci értékének változása az időben.
    Erre épül a grafikon a játékos oldalán.
    """
    __tablename__ = 'fact_market_values'
    mv_id = Column(Integer, primary_key=True, autoincrement=True)
    
    player_id = Column(Integer, ForeignKey('dim_players.player_id'))
    team_id = Column(Integer, ForeignKey('dim_teams.team_id'), nullable=True)
    
    date_recorded = Column(Date)
    market_value_eur = Column(Integer)

class FactTransfer(Base):
    """
    A játékos átigazásoli története.
    Melyik csapattól melyik csapathoz, mikor és mennyiért.
    """
    __tablename__ = 'fact_transfers'
    transfer_id = Column(Integer, primary_key=True, autoincrement=True)
    
    player_id = Column(Integer, ForeignKey('dim_players.player_id'))
    teamFrom_id = Column(Integer, ForeignKey('dim_teams.team_id'), nullable=True)
    teamTo_id = Column(Integer, ForeignKey('dim_teams.team_id'), nullable=True)
    season_id = Column(Integer, ForeignKey('dim_seasons.season_id'))

    date_recorded = Column(Date)
    market_value_eur = Column(Integer)
    fee_eur = Column(Integer)

class FactPlayerSeasonStat(Base):
    """
    A játékos adott szezonbeli összesített statisztikái egy bajnokságban.
    """
    __tablename__ = 'fact_player_season_stats'
    season_stat_id = Column(Integer, primary_key=True, autoincrement=True)
    
    player_id = Column(Integer, ForeignKey('dim_players.player_id'))
    team_id = Column(Integer, ForeignKey('dim_teams.team_id'))
    season_id = Column(Integer, ForeignKey('dim_seasons.season_id'))
    competition_id = Column(Integer, ForeignKey('dim_competitions.competition_id'))

    appearances = Column(Integer)
    goals = Column(Integer)
    assists = Column(Integer)
    yellow_cards = Column(Integer)
    red_cards = Column(Integer)
    minutes_played = Column(Integer)
