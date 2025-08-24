# config_entity.py

import os
from src.taxi_demand.constants import training_pipeline


print(training_pipeline.PIPELINE_NAME)
print(training_pipeline.ARTIFACT_DIR)

class TrainingPipelineConfig:
    def __init__(self):
        self.pipeline_name = training_pipeline.PIPELINE_NAME
        self.artifact_name = training_pipeline.ARTIFACT_DIR
        self.artifact_dir = os.path.join(self.artifact_name)


class DataIngestionConfig:
    def __init__(self, training_pipeline_config: TrainingPipelineConfig):
        self.data_ingestion_dir = os.path.join(training_pipeline_config.artifact_dir,
            training_pipeline.DATA_INGESTION_DIR_NAME
        )
        self.data_ingestion_year = training_pipeline.DATA_INGESTION_YEAR
        self.data_ingestion_tlc_trip_collection_template = training_pipeline.DATA_INGESTION_TLC_TRIP_COLLECTION_TEMPLATE
        self.data_ingestion_tlc_trip_file_template = training_pipeline.DATA_INGESTION_TLC_TRIP_FILE_TEMPLATE

        from src.taxi_demand.constants.training_pipeline import months_to_str
        months_str = months_to_str(training_pipeline.DATA_INGESTION_TLC_TRIP_MONTHS, self.data_ingestion_year)
        self.data_ingestion_weather_collection_name = training_pipeline.DATA_INGESTION_WEATHER_COLLECTION_NAME_TEMPLATE.format(
            month_range=months_str,
            year=self.data_ingestion_year
        )

        self.data_ingestion_tlc_trip_months = training_pipeline.DATA_INGESTION_TLC_TRIP_MONTHS
        self.data_ingestion_feature_store_dir = os.path.join(
            self.data_ingestion_dir,
            training_pipeline.DATA_INGESTION_FEATURE_STORE_DIR
        )
        self.data_ingestion_ingested_dir = os.path.join(
            self.data_ingestion_dir,
            training_pipeline.DATA_INGESTION_INGESTED_DIR
        )
        self.training_file_path = os.path.join(
            self.data_ingestion_ingested_dir,
            training_pipeline.TRAIN_FILE_NAME
        )
        self.testing_file_path = os.path.join(
            self.data_ingestion_ingested_dir,
            training_pipeline.TEST_FILE_NAME
        )
        self.train_test_split_ratio = training_pipeline.DATA_INGESTION_TRAIN_TEST_SPLIT_RATIO


class DataValidationConfig:
    def __init__(self, training_pipeline_config: TrainingPipelineConfig):
        self.data_validation_dir = os.path.join(training_pipeline_config.artifact_dir,
            training_pipeline.DATA_VALIDATION_DIR_NAME
        )
        self.valid_data_dir = os.path.join(
            self.data_validation_dir,
            training_pipeline.DATA_VALIDATION_VALID_DIR
        )
        self.invalid_data_dir = os.path.join(
            self.data_validation_dir,
            training_pipeline.DATA_VALIDATION_INVALID_DIR
        )
        self.valid_train_file_path = os.path.join(
            self.valid_data_dir,
            training_pipeline.TRAIN_FILE_NAME
        )
        self.valid_test_file_path = os.path.join(
            self.valid_data_dir,
            training_pipeline.TEST_FILE_NAME
        )
        self.invalid_train_file_path = os.path.join(
            self.invalid_data_dir,
            training_pipeline.TRAIN_FILE_NAME
        )
        self.invalid_test_file_path = os.path.join(
            self.invalid_data_dir,
            training_pipeline.TEST_FILE_NAME
        )
        self.drift_report_file_path = os.path.join(
            self.data_validation_dir,
            training_pipeline.DATA_VALIDATION_DRIFT_REPORT_DIR,
            training_pipeline.DATA_VALIDATION_DRIFT_REPORT_FILE_NAME
        )