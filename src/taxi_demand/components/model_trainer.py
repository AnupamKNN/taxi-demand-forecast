import os
import sys
import numpy as np
import pandas as pd
import keras_tuner as kt
import tensorflow as tf
from tensorflow.keras.layers import Input, Embedding, Dense, Flatten, Concatenate, Dropout
from tensorflow.keras.models import Model
from tensorflow.keras.optimizers import Adam

from src.taxi_demand.entity.config_entity import ModelTrainerConfig
from src.taxi_demand.entity.artifact_entity import ModelTrainerArtifact, DataTransformationArtifact
from src.taxi_demand.exception.exception import TaxiDemandException
from src.taxi_demand.logging.logger import logging
from src.taxi_demand.utils.main_utils.utils import save_object, load_csv_to_dataframe
from src.taxi_demand.utils.ml_utils.metric.regression_metric import get_regression_score

import mlflow
from dotenv import load_dotenv
load_dotenv()


# Safely fetch credentials from .env
tracking_uri = os.getenv("MLFLOW_TRACKING_URI")
username = os.getenv("MLFLOW_TRACKING_USERNAME")
password = os.getenv("MLFLOW_TRACKING_PASSWORD")

if not all([tracking_uri, username, password]):
    raise ValueError("Missing one or more MLFLOW environment variables. Please check your .env file.")

os.environ['MLFLOW_TRACKING_URI'] = tracking_uri
os.environ['MLFLOW_TRACKING_USERNAME'] = username
os.environ['MLFLOW_TRACKING_PASSWORD'] = password

mlflow.set_tracking_uri(tracking_uri)


class ModelTrainer:
    def __init__(self, model_trainer_config: ModelTrainerConfig, data_transformation_artifact: DataTransformationArtifact):
        try:
            self.model_trainer_config = model_trainer_config
            self.data_transformation_artifact = data_transformation_artifact
        except Exception as e:
            raise TaxiDemandException(e, sys)

    def track_mlflow(self, best_model, train_metrics, test_metrics):
        with mlflow.start_run(run_name="Regression DL Model"):
            mlflow.log_metric("Train MAE", train_metrics.mae)
            mlflow.log_metric("Train MSE", train_metrics.mse)
            mlflow.log_metric("Train RMSE", train_metrics.rmse)
            mlflow.log_metric("Train R2", train_metrics.r2)

            mlflow.log_metric("Test MAE", test_metrics.mae)
            mlflow.log_metric("Test MSE", test_metrics.mse)
            mlflow.log_metric("Test RMSE", test_metrics.rmse)
            mlflow.log_metric("Test R2", test_metrics.r2)

            # mlflow.tensorflow.log_model(best_model, artifact_path =  "model", registered_model_name= "Regression DL Model")

    def build_model(self, hp, categorical_cardinalities):
        inputs = {}
        embeddings = []

        cat_features = list(categorical_cardinalities.keys())
        for col in cat_features:
            inputs[col] = Input(shape=(1,), name=col)
            emb_dim = hp.Int(f"emb_dim_{col}", min_value=2, max_value=50, step=2)
            emb = Embedding(input_dim=categorical_cardinalities[col] + 1, output_dim=emb_dim, name=f"emb_{col}")(inputs[col])
            embeddings.append(Flatten()(emb))

        numeric_cols = ['is_weekend', 'is_holiday', 'is_rain',
                        'temperature_2m', 'precipitation', 'ride_count_lag_1',
                        'ride_count_lag_24', 'ride_count_lag_168',
                        'ride_count_roll_mean_3', 'ride_count_roll_std_3']

        numeric_inputs = Input(shape=(len(numeric_cols),), name='numeric_inputs')
        embeddings.append(numeric_inputs)

        x = Concatenate()(embeddings)

        for i in range(hp.Int('num_dense_layers', 1, 3)):
            units = hp.Int(f"dense_units_{i}", min_value=32, max_value=256, step=32)
            x = Dense(units, activation='relu')(x)
            if hp.Boolean(f"dropout_{i}"):
                rate = hp.Float(f"dropout_rate_{i}", min_value=0.1, max_value=0.5, step=0.1)
                x = Dropout(rate)(x)

        output = Dense(1, activation='linear')(x)

        model = Model(inputs=[*inputs.values(), numeric_inputs], outputs=output)

        lr = hp.Float('learning_rate', 1e-4, 1e-2, sampling='log')
        model.compile(optimizer=Adam(learning_rate=lr), loss='mse', metrics=['mae'])

        return model

    def create_model_inputs(self, X: pd.DataFrame, categorical_cols: list, numeric_cols: list):
        inputs = {col: X[col].values.astype(np.int32) for col in categorical_cols}
        inputs['numeric_inputs'] = X[numeric_cols].values.astype(np.float32)
        return inputs

    def initiate_model_trainer(self) -> ModelTrainerArtifact:
        try:
            # Load transformed CSVs as pandas DataFrames
            train_df = load_csv_to_dataframe(self.data_transformation_artifact.transformed_train_file_path)
            test_df = load_csv_to_dataframe(self.data_transformation_artifact.transformed_test_file_path)

            categorical_cardinalities = {
                'PULocationID': 263,
                'weathercode': 36,
                'hour': 24,
                'day_of_week': 7,
                'month': 12
            }
            cat_cols = list(categorical_cardinalities.keys())
            numeric_cols = ['is_weekend', 'is_holiday', 'is_rain',
                            'temperature_2m', 'precipitation', 'ride_count_lag_1',
                            'ride_count_lag_24', 'ride_count_lag_168',
                            'ride_count_roll_mean_3', 'ride_count_roll_std_3']

            # Prepare inputs for training and validation
            train_inputs = self.create_model_inputs(train_df, cat_cols, numeric_cols)
            val_inputs = self.create_model_inputs(test_df, cat_cols, numeric_cols)

            y_train = train_df['ride_count'].values
            y_test = test_df['ride_count'].values


            tuner = kt.RandomSearch(
                hypermodel=lambda hp: self.build_model(hp, categorical_cardinalities),
                objective = 'val_loss',
                max_trials = 20,
                executions_per_trial = 1, 
                directory = 'tuner_dir',
                project_name = 'taxi_demand_forecasting'
            )

            # tuner = kt.RandomSearch(
            #     hypermodel=lambda hp: self.build_model(hp, categorical_cardinalities),
            #     objective='val_loss',
            #     max_trials=20,
            #     executions_per_trial=1,
            #     directory=self.model_trainer_config.model_trainer_dir,
            #     project_name='ride_count_forecasting',
            #     max_retries_per_trial=3,
            #     max_consecutive_failed_trials=10
            # )

            tuner.search(train_inputs, y_train, validation_data=(val_inputs, y_test), epochs=10, batch_size=1024)

            best_model = tuner.get_best_models(num_models=1)[0]

            model_save_path = self.model_trainer_config.trained_model_file_path
            os.makedirs(os.path.dirname(model_save_path), exist_ok=True)
            best_model.save(model_save_path)

            logging.info(f"Saved best model at: {model_save_path}")

            train_preds = best_model.predict(train_inputs).squeeze()
            test_preds = best_model.predict(val_inputs).squeeze()

            train_metrics = get_regression_score(y_train, train_preds)
            test_metrics = get_regression_score(y_test, test_preds)

            self.track_mlflow(best_model, train_metrics, test_metrics)

            final_models_dir = os.path.join(os.getcwd(), "final_models")
            os.makedirs(final_models_dir, exist_ok=True)
            save_object(os.path.join(final_models_dir, "model.keras"), best_model)

            return ModelTrainerArtifact(
                trained_model_file_path=model_save_path,
                train_metric_artifact=train_metrics,
                test_metric_artifact=test_metrics
            )

        except Exception as e:
            raise TaxiDemandException(e, sys)
