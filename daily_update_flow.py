from prefect import flow, task
from prefect import get_run_logger
import subprocess
import os
import sys

@task(name="Run_Daily_ETL")
def run_daily_etl():
    """
    Lefuttatja az etl_daily.py-t az előző nap eseményeire.
    """
    python_executable = sys.executable
    command = [
        python_executable, 
        os.path.join(os.path.dirname(__file__), "etl_daily.py")
    ]
    
    logger = get_run_logger()
    logger.info("Napi ETL indítása...")
    
    result = subprocess.run(command, capture_output=True, text=True, check=True)
    logger.info(f"stdout: {result.stdout}")
    return result.returncode

@flow(name="Napi_Adatfrissites_00:05")
def daily_update_flow():
    run_daily_etl()

if __name__ == "__main__":
    daily_update_flow.serve(
        name="daily-etl-deployment",
        cron="5 0 * * *"
    )