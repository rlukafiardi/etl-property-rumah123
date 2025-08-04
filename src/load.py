import pandas as pd
from sqlalchemy import create_engine, text, String
import logging


def truncate_staging_table(conn, stg_table: str):
    """Truncates the staging table"""
    logging.info(f"Truncating staging table: {stg_table}")
    conn.execute(text(f"TRUNCATE TABLE {stg_table}"))
    logging.info(f"Staging table {stg_table} truncated")


def insert_to_staging(df: pd.DataFrame, conn, stg_table: str, batch_size: int):
    """Insert data into staging table in chunks"""
    for i in range(0, len(df), batch_size):
        chunk = df.iloc[i:i + batch_size]
        chunk.to_sql(
            stg_table,
            con=conn,
            if_exists='append',
            index=False,
            method='multi',
            chunksize=batch_size
        )
        logging.info(f"Inserted batch {i // batch_size + 1} ({len(chunk)} records) into staging table {stg_table}")
    
    logging.info(f"Inserted {len(df)} records into staging table {stg_table}")


def merge_staging_to_main(df: pd.DataFrame, conn, stg_table: str, main_table: str, unique_key: str):
    """Merges data from the staging table into the main table"""
    logging.info(f"Merging data from {stg_table} into {main_table}")
    merge_query = text(f"""
        INSERT INTO {main_table} ({', '.join(df.columns)})
        SELECT {', '.join(df.columns)} FROM {stg_table}
        ON CONFLICT ({unique_key}) DO UPDATE SET
        {', '.join([f'{col} = EXCLUDED.{col}' for col in df.columns])}
        RETURNING xmax = 0;
    """)
    
    result = conn.execute(merge_query)
    inserted_count = sum(1 for row in result if row[0])
    logging.info(f"Inserted {inserted_count} new records into {main_table}")


def load_to_postgres(
    df: pd.DataFrame,
    conn_str: str,
    stg_table: str,
    main_table: str,
    unique_key: str,
    batch_size: int
):
    """
    Loads a DataFrame into a PostgreSQL database using SQLAlchemy and merges it into the main table.

    Parameters:
        df : pd.DataFrame
            The DataFrame containing the data to load.
        conn_str : str
            SQLAlchemy connection string (e.g., 'postgresql+psycopg2://user:password@host/dbname').
        stg_table : str
            Name of the staging table where data will be temporarily loaded.
        main_table : str
            Name of the main table where data will be merged.
        unique_key : str
            Column name that serves as the unique key for conflict resolution.
        batch_size : int
            Number of records to insert per batch.
    """
    
    if df.empty:
        logging.info("No data to load. Exiting function")
        return

    if not isinstance(batch_size, int) or batch_size <= 0:
        raise ValueError("batch_size must be a positive integer")
    
    try:
        engine = create_engine(conn_str)
        
        with engine.begin() as conn:
            try:
                logging.info("Starting transaction to load data into database")
                truncate_staging_table(conn, stg_table)
                insert_to_staging(df, conn, stg_table, batch_size)
                merge_staging_to_main(df, conn, stg_table, main_table, unique_key)
                logging.info("Transaction committed successfully")
                
            except Exception as e:
                logging.error(f"Error during transaction: {e}")
                raise
    
    except Exception as e:
        logging.error(f"Database operation error: {e}")
        raise