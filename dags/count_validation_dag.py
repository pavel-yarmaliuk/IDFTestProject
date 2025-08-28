import os
import logging
from datetime import timedelta, datetime

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import Variable
import clickhouse_connect

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
    tags=['astros', 'api', 'postgres', 'clickhouse', 'data_validation'],
)
def astros_compare_counts_on_source_and_destination():
        
    @task
    def get_postgres_count():
        """Get PostgreSQL count"""
        hook = PostgresHook(postgres_conn_id="postgres_default")
        
        # Get count of astros in Postgresql
        count_sql = """
            select count(*) from public.astros_data; 
        """
        first_record = hook.get_first(count_sql)
        if first_record:
            return first_record[0]
        else:
            return 0
        
    @task
    def get_clickhouse_count():
        """Get Clickhouse count"""

        client = clickhouse_connect.get_client(host=Variable.get('clickhouse_host', 'localhost'), 
                                               username=Variable.get('clickhouse_username', 'default'), 
                                               password=Variable.get('clickhouse_password',''))
        
        # Get count of astros in Clickhouse
        count_sql = "select count(*) from kafka.astros_data;"
        first_record = client.query(count_sql).result_rows[0]
        if first_record:
            return first_record[0]
        else:
            return 0
    @task
    def compare_counts(clickhouse_count, postgres_count):
        if clickhouse_count != postgres_count:
            raise Exception(f'Counts not matched: \nClickhouse:{clickhouse_count}\nPostgres:{postgres_count}')
        else:
            LOGGER.info(f'Counts matched: \nClickhouse:{clickhouse_count}\nPostgres:{postgres_count}')

    postgres_count = get_postgres_count()
    clickhouse_count = get_clickhouse_count()
    compared_counts = compare_counts(clickhouse_count, postgres_count)

# DAG Initialization
validation_dag = astros_compare_counts_on_source_and_destination()