import pandas as pd
import logging


def drop_null_and_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """Drops rows with null values in the 'link' column and removes duplicate rows based on that column"""
    logging.info("Dropping rows with null values in 'link' column")
    df = df.dropna(subset=['link'])

    logging.info("Removing duplicate rows based on 'link' column")
    df = df.drop_duplicates(subset='link')

    return df


def extract_numeric_sizes(df: pd.DataFrame) -> pd.DataFrame:
    """Extracts numeric values from 'lot_size' and 'building_size' columns"""
    logging.info("Extracting numeric values from 'lot_size' and 'building_size' columns")
    df['lot_size'] = df['lot_size'].str.extract(r'(\d+)')
    df['building_size'] = df['building_size'].str.extract(r'(\d+)')
    
    return df


def parse_price(price: str) -> float:
    """Cleans and converts the 'price_rp' column from string to numerical format"""
    if not isinstance(price, str):
        return None
    
    try:
        if "triliun" in price:
            price = float(price.replace(" triliun", "")) * 1_000_000_000_000
        elif "miliar" in price:
            price = float(price.replace(" miliar", "")) * 1_000_000_000
        elif "juta" in price:
            price = float(price.replace(" juta", "")) * 1_000_000
        elif "ribu" in price:
            price = float(price.replace(" ribu", "")) * 1_000
    except ValueError:
        logging.error(f"Error parsing price: {price}")
        return None
    
    return price


def clean_price_column(df: pd.DataFrame) -> pd.DataFrame:
    """Cleans and standardizes the 'price_rp' column"""
    logging.info("Cleaning and standardizing 'price_rp' column")
    df['price_rp'] = df['price_rp'].str.lower().str.replace('rp ', '').str.replace(',', '.').str.strip()

    df['price_rp'] = df['price_rp'].map(parse_price).round(0).astype("Int64")

    return df


def cast_columns_to_int(df: pd.DataFrame) -> pd.DataFrame:
    """Casts specific columns to appropriate data types."""
    logging.info("Casting columns to appropriate data types")
    columns_to_cast = [
        "n_bedroom", "n_bathroom", "n_carport",
        "lot_size", "building_size"
    ]

    for column in columns_to_cast:
        df[column] = pd.to_numeric(df[column], errors='coerce').astype("Int64")
    
    return df


def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the input DataFrame by performing the following operations:
    1. Drops rows with null values in the 'link' column.
    2. Removes duplicate rows based on the 'link' column.
    3. Extracts numeric values from 'lot_size' and 'building_size' columns.
    4. Cleans and converts the 'price_rp' column from string to numerical format.
    5. Casts specific columns to appropriate data types.
    
    Parameters:
        df (pd.DataFrame): Input a DataFrame.
    
    Returns:
        pd.DataFrame: Transformed DataFrame.
    """
    logging.info(f"Initial DataFrame shape: {df.shape}")
    
    df = drop_null_and_duplicates(df)
    df = extract_numeric_sizes(df)
    df = clean_price_column(df)
    df = cast_columns_to_int(df)
    
    logging.info(f"Final DataFrame shape: {df.shape}")
    logging.info(f"Transformed {len(df)} records")

    return df
