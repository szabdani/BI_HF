from prefect import flow, task
from prefect import get_run_logger
import subprocess
import os
import sys

@task(name="Load_Season_Data")
def run_season_load(competition: str, year: int):
    """
    Lefuttatja az etl_season_load.py-t az adott paraméterekkel.
    """
    python_executable = sys.executable 
    command = [
        python_executable, 
        os.path.join(os.path.dirname(__file__), "etl_season_load.py"), 
        "-c", competition, 
        "-y", str(year)
    ]
    
    logger = get_run_logger()
    logger.info(f"Futtatás indítása: {' '.join(command)}")
    
    # A subprocess segítségével futtatjuk a külső Python szkriptet
    result = subprocess.run(command, capture_output=True, text=True, check=True)
    
    logger.info(f"stdout: {result.stdout}")
    if result.stderr:
        logger.error(f"stderr: {result.stderr}")
        
    return result.returncode

@task(name="Load_Player_Details")
def run_player_details():
    """
    Lefuttatja az etl_player_data.py-t a már betöltött játékosokra.
    """
    python_executable = sys.executable
    command = [
        python_executable, 
        os.path.join(os.path.dirname(__file__), "etl_player_data.py"),
        "-l 2" # Limit 2-re, hogy le is tudjon futni rendesen
    ]
    
    logger = get_run_logger()
    logger.info(f"Futtatás indítása: {' '.join(command)}")
    
    result = subprocess.run(command, capture_output=True, text=True, check=True)
    logger.info(f"stdout: {result.stdout}")
    return result.returncode

@flow(name="Load_PL_2025")
def initial_setup_flow(competition: str = "PL", year: int = 2025):
    season_result = run_season_load(competition, year)
    
    # Csak akkor futtatjuk a kiegészítő adatokat, ha a szezon betöltés sikeres volt
    if season_result == 0:
        run_player_details()

if __name__ == "__main__":
    initial_setup_flow()