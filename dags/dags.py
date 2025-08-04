from airflow.decorators import dag, task
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import datetime, timedelta
import logging
import pandas as pd
import os

from src.extract import extract_data
from src.transform import transform_data
from src.load import load_to_postgres
from utils.helper import read_config, save_to_csv


# Default arguments for the DAG
default_args = {
    'owner': 'etl_data',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Read extract configuration
extract_cfg = read_config("extract.yaml")
regions = extract_cfg["regions"]
ads_type = extract_cfg["ads_type"]
property_type = extract_cfg["property_type"]
num_pages = extract_cfg["num_pages"]

# Read load configuration
loading_cfg = read_config("load.yaml")
stg_table = loading_cfg["stg_table"]
main_table = loading_cfg["main_table"]
unique_key = loading_cfg["unique_key"]
batch_size = loading_cfg["batch_size"]

def create_dag(region_config: dict):
    region_name = region_config['name']
    dag_id = f"etl_property_{region_name}"

    @dag(
        dag_id=dag_id,
        default_args=default_args,
        schedule=region_config['schedule'],
        start_date=datetime(2025, 8, 1),
        catchup=False,
        tags=['etl', 'property', region_name]
    )
    def dynamic_etl_dag():

        @task
        def extract_task():
            """Extract task for the ETL pipeline"""
            logging.info("-" * 100)
            logging.info("Extracting property data...")

            region_id = region_config['id']
            admin_list = region_config['admins']
            
            # Perform extraction
            raw_data = extract_data(
                ads_type,
                region_id,
                property_type,
                num_pages,
                admin_list
            )

            filename = f"data_{region_name}_{property_type}_{ads_type}"
            raw_data_path = save_to_csv(raw_data, filename, filepath='./data/raw')
            logging.info(f"Extracted data saved to {raw_data_path}")
            logging.info("-" * 100)
            
            return raw_data_path
        
        @task
        def transform_task(raw_data_path: str):
            """Transform task for the ETL pipeline"""
            logging.info("-" * 100)
            logging.info(f"Reading raw data from {raw_data_path} for transformation...")
            raw_data = pd.read_csv(raw_data_path)

            # Perform transformation
            logging.info("Transforming property data...")
            processed_data = transform_data(raw_data)

            filename = f"data_{region_name}_{property_type}_{ads_type}"
            processed_data_path = save_to_csv(processed_data, filename, filepath='./data/processed')
            logging.info(f"Processed data saved to {processed_data_path}")
            logging.info("-" * 100)
            
            return processed_data_path
        
        @task
        def load_task(processed_data_path: str):
            """Load task for the ETL pipeline"""
            logging.info("-" * 100)
            logging.info(f"Reading transformed data from {processed_data_path}")
            processed_data = pd.read_csv(processed_data_path)

            logging.info("Loading data into database...")
            
            # Database connection parameters
            hook = PostgresHook(postgres_conn_id='POSTGRES_ETL_DB')
            conn_str = hook.get_uri()
            
            # Perform loading
            load_to_postgres(
                processed_data,
                conn_str,
                stg_table,
                main_table,
                unique_key,
                batch_size
            )
            logging.info(f"Data successfully loaded into {loading_cfg['main_table']}")
        
        @task(trigger_rule=TriggerRule.ALL_DONE)
        def cleanup_files_task(raw_data_path: str, processed_data_path: str):
            logging.info("-" * 100)
            logging.info("Cleaning up raw and processed data files...")
            
            for path in [raw_data_path, processed_data_path]:
                try:
                    if os.path.exists(path):
                        os.remove(path)
                        logging.info(f"Removed file: {path}")
                    else:
                        logging.warning(f"File not found, skipping: {path}")
                except Exception as e:
                    logging.error(f"Failed to remove file {path}: {e}", exc_info=True)
        
        # Define task dependencies
        extract = extract_task()
        transform = transform_task(extract)
        load = load_task(transform)
        cleanup = cleanup_files_task(extract, transform)

        # Set task dependencies explicitly
        extract >> transform >> load >> cleanup
    
    return dynamic_etl_dag()

for region in regions:
    dag_id = f"etl_property_{region['name']}"
    globals()[dag_id] = create_dag(region)