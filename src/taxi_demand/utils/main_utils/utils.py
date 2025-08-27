import yaml
from src.taxi_demand.exception.exception import TaxiDemandException
from src.taxi_demand.logging.logger import logging

import numpy as np
import pandas as pd
import pickle

import os, sys

def read_yaml_file(file_path: str) -> dict:
    """
    Reads a YAML file and returns its contents as a dictionary.

    Args:
        file_path (str): The path to the YAML file.

    Returns:
        dict: The contents of the YAML file as a dictionary.

    Raises:
        TaxiDemandException: If there is an error reading the file.
    """
    try:
        with open(file_path, 'r') as file:
            content = yaml.safe_load(file)
        logging.info(f"YAML file '{file_path}' read successfully.")
        return content
    except Exception as e:
        raise TaxiDemandException(e, sys) from e
    
def write_yaml_file(file_path: str, content: object, replace: bool = False) -> None:
    """
    Writes a dictionary to a YAML file.

    Args:
        file_path (str): The path to the YAML file.
        content (dict): The dictionary to write to the file.

    Raises:
        TaxiDemandException: If there is an error writing to the file.
    """
    try:
        if replace:
            if os.path.exists(file_path):
                os.remove(file_path)
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, 'w') as file:
            yaml.safe_dump(content, file)
        logging.info(f"YAML file '{file_path}' written successfully.")
    except Exception as e:
        raise TaxiDemandException(e, sys) from e
    
def save_dataframe_to_csv(file_path: str, df: pd.DataFrame):
    try:
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        df.to_csv(file_path, index=False)
    except Exception as e:
        raise TaxiDemandException(e, sys)

def load_csv_to_dataframe(file_path: str) -> pd.DataFrame:
    try:
        return pd.read_csv(file_path)
    except Exception as e:
        raise TaxiDemandException(e, sys)

    
def save_object(file_path: str, obj: object) -> None:
    try:
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, 'wb') as file_obj:
            pickle.dump(obj, file_obj)
    except Exception as e:
        raise TaxiDemandException(e, sys)
    
def load_object(file_path: str) -> object:
    try:
        if not os.path.exists(file_path):
            raise Exception(f"The file: {file_path} does not exist.")
        with open(file_path, 'rb') as file_obj:
            return pickle.load(file_obj)
    except Exception as e:
        raise TaxiDemandException(e, sys)

