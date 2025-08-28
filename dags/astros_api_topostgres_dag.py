from datetime import datetime, timedelta
from typing import List, Tuple, Dict, Any
import logging
import json

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests
from tenacity import retry, wait_exponential

LOGGER = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    default_args=default_args,
    schedule=timedelta(hours=6),
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['astros', 'api', 'postgres'],
)
def astros_api_to_postgres():
    
    @retry(wait=wait_exponential(multiplier=1, min=4, max=10), reraise=True)
    @task
    def fetch_astros_data() -> Dict[str, Any]:
        """Fetching data about astronauts"""
        try:
            api_url = "http://api.open-notify.org/astros.json"
            response = requests.get(api_url, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            LOGGER.info(f"Exception on fetching data: {str(e)}")
            raise
        except json.JSONDecodeError as e:
            LOGGER.info(f"Parsing json exception: {str(e)}")

    @task
    def transform_astros_data(raw_data: Dict[str, Any]) -> List[Tuple[str, str]]:
        """Transorming astronauts data"""
        if 'people' not in raw_data:
            raise ValueError("Invalid data format: missing 'people' key")
        
        transformed = []
        for astronaut in raw_data['people']:
            transformed.append((
                astronaut.get('craft', 'Unknown'),
                astronaut.get('name', 'Unknown')
            ))
        
        LOGGER.info(f"Transformed {len(transformed)} astronaut records")
        return transformed
    
    @task
    def load_astros_data(astronauts: List[Tuple[str, str]]):
        """Data loading in PostgreSQL"""
        hook = PostgresHook(postgres_conn_id="postgres_default")
        
        # Table creation
        create_sql = """
            CREATE TABLE IF NOT EXISTS astros_data (
                id SERIAL PRIMARY KEY,
                craft VARCHAR(100),
                name VARCHAR(255),
                _inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(craft, name)
            )
        """
        hook.run(create_sql)
        
        # Data insertion
        if astronauts:
            insert_sql = """
                INSERT INTO astros_data (craft, name)
                VALUES (%s, %s)
                ON CONFLICT (craft, name) DO UPDATE SET
                    _inserted_at = EXCLUDED._inserted_at
            """
            
            hook.insert_rows(
                table='astros_data',
                rows=astronauts,
                target_fields=['craft', 'name'],
                replace=False,
                commit_every=1000
            )
        
        return f"Loaded {len(astronauts)} astronaut records"
    
    @task
    def log_astros_info(raw_data: Dict[str, Any]):
        """Logging information about astronauts"""
        number = raw_data.get('number', 0)
        message = raw_data.get('message', 'success')
        
        LOGGER.info(f"Astros API Info:")
        LOGGER.info(f"Message: {message}")
        LOGGER.info(f"Total people in space: {number}")
        
        return f"Successfully processed {number} astronauts"
    
    raw_data = fetch_astros_data()
    transformed_data = transform_astros_data(raw_data)
    load_result = load_astros_data(transformed_data)
    log_info = log_astros_info(raw_data)

# DAG Initialization
astros_dag = astros_api_to_postgres()