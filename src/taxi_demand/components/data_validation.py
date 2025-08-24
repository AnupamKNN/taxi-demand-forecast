from src.taxi_demand.entity.artifact_entity import DataIngestionArtifact, DataValidationArtifact
from src.taxi_demand.entity.config_entity import DataValidationConfig
from src.taxi_demand.exception.exception import TaxiDemandException
from src.taxi_demand.logging.logger import logging
from src.taxi_demand.constants.training_pipeline import SCHEMA_FILE_PATH
from src.taxi_demand.utils.main_utils.utils import read_yaml_file, write_yaml_file

from scipy.stats import ks_2samp
import pandas as pd
import os
import sys


class DataValidation:
    def __init__(self, data_ingestion_artifact: DataIngestionArtifact,
                 data_validation_config: DataValidationConfig):
        try:
            self.data_ingestion_artifact = data_ingestion_artifact
            self.data_validation_config = data_validation_config
            self.schema = read_yaml_file(SCHEMA_FILE_PATH)
        except Exception as e:
            raise TaxiDemandException(e, sys) from e

    @staticmethod
    def read_data(file_path) -> pd.DataFrame:
        try:
            return pd.read_csv(file_path)
        except Exception as e:
            raise TaxiDemandException(e, sys) from e

    def validate_number_of_columns(self, dataframe: pd.DataFrame) -> bool:
        try:
            number_of_columns = len(self.schema['columns'])
            logging.info(f"Required number of columns: {number_of_columns}")
            logging.info(f"Dataframe has columns: {dataframe.shape[1]}")
            return len(dataframe.columns) == number_of_columns
        except Exception as e:
            raise TaxiDemandException(e, sys) from e

    def detect_dataset_drift(self, base_df, current_df, threshold=0.05) -> bool:
        try:
            status = True
            report = {}

            for column in base_df.columns:
                d1 = base_df[column]
                d2 = current_df[column]
                is_sample_dist = ks_2samp(d1, d2)
                if is_sample_dist.pvalue < threshold:
                    is_found = True
                    status = False
                else:
                    is_found = False
                report[column] = {
                    "p_value": float(is_sample_dist.pvalue),
                    "drift_status": is_found
                }

            drift_report_file_path = self.data_validation_config.drift_report_file_path
            dir_path = os.path.dirname(drift_report_file_path)
            os.makedirs(dir_path, exist_ok=True)
            write_yaml_file(file_path=drift_report_file_path, content=report)
            return status
        except Exception as e:
            raise TaxiDemandException(e, sys) from e

    def is_numeric_column(self, dataframe: pd.DataFrame, column_name: str) -> bool:
        try:
            return dataframe[column_name].dtype in ['int64', 'float64']
        except Exception as e:
            raise TaxiDemandException(e, sys) from e

    def is_categorical_column(self, dataframe: pd.DataFrame, column_name: str) -> bool:
        try:
            return dataframe[column_name].dtype == 'object'
        except Exception as e:
            raise TaxiDemandException(e, sys) from e

    def initiate_data_validation(self) -> DataValidationArtifact:
        try:
            train_file_path = self.data_ingestion_artifact.train_file_path
            test_file_path = self.data_ingestion_artifact.test_file_path

            train_dataframe = self.read_data(train_file_path)
            test_dataframe = self.read_data(test_file_path)

            # Validate number of columns
            if not self.validate_number_of_columns(train_dataframe):
                raise Exception("Train dataframe does not have all required columns")

            if not self.validate_number_of_columns(test_dataframe):
                raise Exception("Test dataframe does not have all required columns")

            # Check numeric columns presence in all required columns based on schema
            error_message_num = []

            for col, dtype in self.schema['columns'].items():
                if dtype in ['int64', 'float64']:
                    if col not in train_dataframe.columns or not self.is_numeric_column(train_dataframe, col):
                        error_message_num.append(f"Train dataframe column '{col}' is not numeric or missing")
                        
                    if col not in test_dataframe.columns or not self.is_numeric_column(test_dataframe, col):
                        error_message_num.append(f"Test dataframe column '{col}' is not numeric or missing")

            if error_message_num:
                raise Exception("\n".join(error_message_num))
            else:
                logging.info("Numeric columns validation completed successfully. No errors found.")

            # Check categorical columns presence in all required columns based on schema
            error_message_cat = []

            for col, dtype in self.schema['columns'].items():
                if dtype == 'object':
                    if col not in train_dataframe.columns or not self.is_categorical_column(train_dataframe, col):
                        error_message_cat.append(f"Train dataframe column '{col}' is not categorical or missing")

                    if col not in test_dataframe.columns or not self.is_categorical_column(test_dataframe, col):
                        error_message_cat.append(f"Test dataframe column '{col}' is not categorical or missing")

            if error_message_cat:
                raise Exception("\n".join(error_message_cat))
            else:
                logging.info("Categorical columns validation completed successfully. No errors found.")

            # Detect dataset drift
            drift_status = self.detect_dataset_drift(base_df=train_dataframe, current_df=test_dataframe)
            os.makedirs(os.path.dirname(self.data_validation_config.valid_train_file_path), exist_ok=True)

            data_validation_artifact = DataValidationArtifact(
                validation_status=drift_status,
                valid_train_file_path=self.data_validation_config.valid_train_file_path,
                valid_test_file_path=self.data_validation_config.valid_test_file_path,
                invalid_train_file_path=self.data_validation_config.invalid_train_file_path,
                invalid_test_file_path=self.data_validation_config.invalid_test_file_path,
                drift_report_file_path=self.data_validation_config.drift_report_file_path
            )

            return data_validation_artifact

        except Exception as e:
            raise TaxiDemandException(e, sys) from e
