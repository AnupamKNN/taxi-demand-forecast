import os
import sys
from typing import List


"""
Defining common constant variables for training pipeline
"""

TARGET_COLUMN= "ride_count"
PIPELINE_NAME = "Taxi/Ride-share Demand Forecasting & Supply Optimization"
ARTIFACT_DIR: str = "Artifact"
FILE_NAME: List[str] = ["nyc_weather_jan_mar_2025.json", "yellow_tripdata_2025-01.parquet", "yellow_tripdata_2025-02.parquet", "yellow_tripdata_2025-03.parquet"]

TRAIN_FILE_NAME: str = "train.csv"
TEST_FILE_NAME: str = "test.csv"

SCHEMA_FILE_PATH = os.path.join("data_schema", "schema.yaml")
MODEL_FILE_PATH = "model.pkl"




"""
Data Ingestion related constants start with DATA_INGESTION VAR NAME configured for dynamic month/year range
"""

PIPELINE_NAME: str = "Taxi/Ride-share Demand Forecasting & Supply Optimization"
ARTIFACT_DIR: str = "Artifact"
DATA_INGESTION_DIR_NAME: str = "data_ingestion"

DATA_INGESTION_YEAR: int = 2025

# List months to ingest (can expand/change as needed)
DATA_INGESTION_TLC_TRIP_MONTHS = [1, 2, 3, 4]  # January-February-March (example)

DATA_INGESTION_TLC_TRIP_COLLECTION_TEMPLATE: str = "yellow_tripdata_{year}-{month:02d}"
DATA_INGESTION_TLC_TRIP_FILE_TEMPLATE: str = "yellow_tripdata_{year}-{month:02d}.parquet"

# Weather collection name will be dynamically generated with helper
DATA_INGESTION_WEATHER_COLLECTION_NAME_TEMPLATE: str = "nyc_weather_{month_range}_{year}"

DATA_INGESTION_FEATURE_STORE_DIR: str = "feature_store"
DATA_INGESTION_INGESTED_DIR: str = "ingested"

DATA_INGESTION_TRAIN_TEST_SPLIT_RATIO: float = 0.2


def months_to_str(months, year):
    import calendar
    if not months:
        return ""
    start_month_name = calendar.month_abbr[months[0]].lower()
    end_month_name = calendar.month_abbr[months[-1]].lower()
    if start_month_name == end_month_name:
        return f"{start_month_name}_{year}"
    else:
        return f"{start_month_name}_{end_month_name}_{year}"


"""
Data Validation related constants start with DATA_VALIDATION VAR NAME
"""

DATA_VALIDATION_DIR_NAME: str = "data_validation"
DATA_VALIDATION_VALID_DIR: str = "validated"
DATA_VALIDATION_INVALID_DIR: str = "invalid"
DATA_VALIDATION_DRIFT_REPORT_DIR: str = "drift_report"
DATA_VALIDATION_DRIFT_REPORT_FILE_NAME: str = "report.yaml"


"""
Data Transformation related constants start with DATA_TRANSFORMATION VAR NAME
"""

DATA_TRANSFORMATION_DIR_NAME = "data_transformation"
DATA_TRANSFORMATION_TRANSFORMED_DIR = "transformed"
DATA_TRANSFORMATION_PREPROCESSOR_OBJECT_DIR = "preprocessor"
PREPROCESSOR_OBJECT_FILE_NAME = "preprocessor.pkl"


"""
Model Trainer related constants start with MODEL_TRAINER VAR NAME
"""

MODEL_TRAINER_DIR_NAME = "model_trainer"
MODEL_TRAINER_TRAINED_MODEL_DIR = "trained_model"
MODEL_TRAINER_TRAINED_MODEL_NAME = "model.keras"
MODEL_TRAINER_EXPECTED_SCORE = 0.85
MODEL_TRAINER_OVER_FITTING_UNDER_FITTING_THRESHOLD = 0.05

"""
Model Pusher related constants start with MODEL_PUSHER VAR NAME
"""

MODEL_PUSHER_DIR = "final_models"
MODEL_PUSHER_LABEL_ENCODER_DIR = "label_encoders"
MODEL_PUSHER_SCALER_DIR = "scaler"
MODEL_PUSHER_MODEL_NAME = "model.pkl"