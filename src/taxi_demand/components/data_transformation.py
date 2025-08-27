import os
import sys
import shutil
import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OrdinalEncoder, StandardScaler

from src.taxi_demand.entity.config_entity import DataTransformationConfig
from src.taxi_demand.entity.artifact_entity import DataTransformationArtifact, DataValidationArtifact
from src.taxi_demand.exception.exception import TaxiDemandException
from src.taxi_demand.logging.logger import logging
from src.taxi_demand.utils.main_utils.utils import save_object, save_dataframe_to_csv, read_yaml_file
from src.taxi_demand.constants.training_pipeline import SCHEMA_FILE_PATH


class DataTransformation:
    def __init__(self, data_transformation_config: DataTransformationConfig, data_validation_artifact: DataValidationArtifact):
        try:
            self.data_transformation_config = data_transformation_config
            self.data_validation_artifact = data_validation_artifact
            self.schema = read_yaml_file(SCHEMA_FILE_PATH)
            self.categorical_cols = [list(col.keys())[0] for col in self.schema.get("categorical_columns", [])]
            self.numerical_cols = [list(col.keys())[0] for col in self.schema.get("numerical_columns", [])]
            self.target_column = list(self.schema['target_column'][0].keys())[0]

        except Exception as e:
            raise TaxiDemandException(e, sys)
        
    @staticmethod
    def read_data(file_path) -> pd.DataFrame:
        try:
            df = pd.read_csv(file_path)
            # Convert categorical columns dtype to int
            categorical_cols = [list(col.keys())[0] for col in read_yaml_file(SCHEMA_FILE_PATH).get("categorical_columns", [])]
            for col in categorical_cols:
                if col in df.columns:
                # Convert to integer, coercing if necessary (adjust 'errors' param as per data)
                    df[col] = df[col].astype(int)
            return df
        except Exception as e:
            raise TaxiDemandException(e, sys)
        
    def get_data_transformation_object(self):
        try:
            logging.info("Creating ColumnTransformer with OrdinalEncoder and StandardScaler")
            preprocessor = ColumnTransformer(
                transformers = [
                    ("cat", OrdinalEncoder(), self.categorical_cols),
                    ("num", StandardScaler(), self.numerical_cols)
                ],
                remainder = "passthrough"
            )
            return preprocessor
        except Exception as e:
            raise TaxiDemandException(e, sys)
        
    def initiate_data_transformation(self) -> DataTransformationArtifact:
        try:
            logging.info("Entered initiate_data_transformation method of Data Transformation class")

            # Read training and testing data
            train_df = self.read_data(self.data_validation_artifact.valid_train_file_path)
            test_df = self.read_data(self.data_validation_artifact.valid_test_file_path)
            
            # Filling missing values in train and test data
            train_df.fillna(method='ffill', inplace=True)
            train_df.fillna(0, inplace=True)

            test_df.fillna(method='ffill', inplace=True)
            test_df.fillna(0, inplace=True)

            # Extract input features and target
            input_feature_train_df = train_df.drop(columns=[self.target_column])
            target_feature_train_df = train_df[self.target_column].values

            input_feature_test_df = test_df.drop(columns=[self.target_column])
            target_feature_test_df = test_df[self.target_column].values

            # Identify expected columns
            expected_columns = self.categorical_cols + self.numerical_cols

            # Log missing/extra columns
            missing_train = set(expected_columns) - set(input_feature_train_df.columns)
            extra_train = set(input_feature_train_df.columns) - set(expected_columns)
            if missing_train:
                logging.warning(f"Missing columns in training data: {missing_train}")
            if extra_train:
                logging.info(f"Extra columns in training data: {extra_train}")

            missing_test = set(expected_columns) - set(input_feature_test_df.columns)
            extra_test = set(input_feature_test_df.columns) - set(expected_columns)
            if missing_test:
                logging.warning(f"Missing columns in testing data: {missing_test}")
            if extra_test:
                logging.info(f"Extra columns in testing data: {extra_test}")

            # Reorder columns to match expected
            input_feature_train_df = input_feature_train_df[expected_columns]
            input_feature_test_df = input_feature_test_df[expected_columns]

            # Get preprocessor and fit on training data
            preprocessor = self.get_data_transformation_object()
            preprocessor_object = preprocessor.fit(input_feature_train_df)

            # Transform training and testing data
            transformed_input_train_feature = preprocessor_object.transform(input_feature_train_df)
            transformed_input_test_feature = preprocessor_object.transform(input_feature_test_df)

            # Convert from sparse matrix to dense ndarray if needed
            if hasattr(transformed_input_train_feature, "toarray"):
                transformed_input_train_feature = transformed_input_train_feature.toarray()
            if hasattr(transformed_input_test_feature, "toarray"):
                transformed_input_test_feature = transformed_input_test_feature.toarray()

            # Convert transformed arrays back to DataFrames with columns
            train_features_df = pd.DataFrame(transformed_input_train_feature, columns=expected_columns)
            test_features_df = pd.DataFrame(transformed_input_test_feature, columns=expected_columns)

            # Add target column back
            train_features_df[self.target_column] = target_feature_train_df
            test_features_df[self.target_column] = target_feature_test_df

            # Save as CSV files (update paths to .csv)
            train_csv_path = self.data_transformation_config.transformed_train_file_path.replace('.npy', '.csv')
            test_csv_path = self.data_transformation_config.transformed_test_file_path.replace('.npy', '.csv')

            train_features_df.to_csv(train_csv_path, index=False)
            test_features_df.to_csv(test_csv_path, index=False)

            # Save preprocessor object for reuse
            save_object(self.data_transformation_config.transformed_object_file_path, preprocessor_object)

            # Copy preprocessor to final models dir for deployment
            final_models_dir = os.path.join(os.getcwd(), "final_models")
            os.makedirs(final_models_dir, exist_ok=True)
            shutil.copy(self.data_transformation_config.transformed_object_file_path,
                        os.path.join(final_models_dir, "preprocessor.pkl"))

            logging.info("Completed data transformation and saved preprocessors and CSV files")

            return DataTransformationArtifact(
                transformed_train_file_path=train_csv_path,
                transformed_test_file_path=test_csv_path,
                transformed_object_file_path=self.data_transformation_config.transformed_object_file_path,
            )
        except Exception as e:
            raise TaxiDemandException(e, sys)
