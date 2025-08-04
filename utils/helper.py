import os
import yaml
import pandas as pd
from datetime import datetime
from typing import Dict


def read_config(config_file: str) -> Dict:
    """
    Reads configuration values from a YAML file inside the project's 'configs' directory.

    Parameters:
        config_file (str): Name of the YAML file (without the path).

    Returns:
        dict: Dictionary with configuration values.
    """
    config = {}

    try:
        # Get the project root directory (assumes script is run from within the project)
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

        # Construct the full path to the config file inside 'configs' folder
        config_path = os.path.join(project_root, "configs", config_file)

        # Ensure the config file exists
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Config file not found: {config_path}")

        # Read the YAML configuration file
        with open(config_path, "r") as f:
            config = yaml.safe_load(f)

    except Exception as e:
        print(f"Error reading config file: {e}")
        exit(1)  # Exit if config reading fails

    return config


def save_to_csv(df: pd.DataFrame, filename: str, filepath: str) -> str:
    """
    Saves the given DataFrame in csv format.

    Parameters:
        df (pd.DataFrame): The DataFrame to be saved.
        filename (str): The name of the file (without extension).
        filepath (str): The directory path where the file will be saved.

    Returns:
        str: The full file path of the saved file.
    """
    # Ensure the output directory exists
    os.makedirs(filepath, exist_ok=True)
    
    # Generate full file path
    date = datetime.now().strftime("%Y%m%d")
    full_path = os.path.join(filepath, f"{filename}_{date}.csv")

    df.to_csv(full_path, index=False)
    
    return full_path
