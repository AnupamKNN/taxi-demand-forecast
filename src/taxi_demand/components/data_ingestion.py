import os
import sys
import pandas as pd
import requests
from sklearn.model_selection import train_test_split

from src.taxi_demand.exception.exception import TaxiDemandException
from src.taxi_demand.logging.logger import logging
from src.taxi_demand.entity.config_entity import DataIngestionConfig
from src.taxi_demand.entity.artifact_entity import DataIngestionArtifact


class DataIngestion:
    def __init__(self, data_ingestion_config: DataIngestionConfig):
        try:
            self.data_ingestion_config = data_ingestion_config
        except Exception as e:
            raise TaxiDemandException(e, sys)

    def fetch_weather_data(self):
        try:
            year = self.data_ingestion_config.data_ingestion_year
            months = sorted(self.data_ingestion_config.data_ingestion_tlc_trip_months)
            month_days = {1:31, 2:28, 3:31, 4:30, 5:31, 6:30,
                          7:31, 8:31, 9:30, 10:31, 11:30, 12:31}
            start_month = months[0]
            end_month = months[-1]
            start_date = f"{year}-{start_month:02d}-01"
            end_date = f"{year}-{end_month:02d}-{month_days[end_month]}"
            url = (
                f"https://archive-api.open-meteo.com/v1/archive?"
                f"latitude=40.7128&longitude=-74.0060&start_date={start_date}&end_date={end_date}"
                f"&hourly=temperature_2m,precipitation,weathercode&timezone=America/New_York"
            )

            logging.info(f"Fetching weather data from {url}")
            response = requests.get(url)
            response.raise_for_status()
            weather_data = response.json()

            df_weather = pd.DataFrame({
                'datetime': weather_data['hourly']['time'],
                'temperature_2m': weather_data['hourly']['temperature_2m'],
                'precipitation': weather_data['hourly']['precipitation'],
                'weathercode': weather_data['hourly']['weathercode']
            })
            df_weather['datetime'] = pd.to_datetime(df_weather['datetime'])
            
            feature_store_dir = self.data_ingestion_config.data_ingestion_feature_store_dir
            os.makedirs(feature_store_dir, exist_ok=True)
            weather_csv_path = os.path.join(feature_store_dir, f"{self.data_ingestion_config.data_ingestion_weather_collection_name}.csv")
            df_weather.to_csv(weather_csv_path, index=False)
            logging.info(f"Weather data saved to {weather_csv_path}")
            return weather_csv_path
        except Exception as e:
            raise TaxiDemandException(f"Failed to fetch/save weather data: {e}", sys)

    def fetch_tlc_trip_data(self):
        try:
            base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data"
            feature_store_dir = self.data_ingestion_config.data_ingestion_feature_store_dir
            os.makedirs(feature_store_dir, exist_ok=True)

            saved_files = []
            for month in self.data_ingestion_config.data_ingestion_tlc_trip_months:
                file_name = self.data_ingestion_config.data_ingestion_tlc_trip_file_template.format(
                    year=self.data_ingestion_config.data_ingestion_year,
                    month=month
                )
                url = f"{base_url}/{file_name}"
                save_path = os.path.join(feature_store_dir, file_name)

                if not os.path.exists(save_path):
                    logging.info(f"Downloading {url} ...")
                    r = requests.get(url, stream=True)
                    r.raise_for_status()
                    with open(save_path, "wb") as f:
                        for chunk in r.iter_content(chunk_size=8192):
                            f.write(chunk)
                    logging.info(f"Saved trip data to {save_path}")
                else:
                    logging.info(f"File {save_path} already exists, skipping download.")

                saved_files.append(save_path)
            return saved_files
        except Exception as e:
            raise TaxiDemandException(f"Failed to fetch TLC trip data: {e}", sys)

    def load_and_merge_datasets(self, trip_files, weather_csv_path):
        try:
            df_taxi = pd.concat(pd.read_parquet(f) for f in trip_files)
            logging.info(f"Concatenated taxi data shape: {df_taxi.shape}")

            df_taxi['tpep_pickup_datetime'] = pd.to_datetime(df_taxi['tpep_pickup_datetime'])
            df_taxi['pickup_hour'] = df_taxi['tpep_pickup_datetime'].dt.floor('h').dt.tz_localize('America/New_York')

            agg_taxi = df_taxi.groupby(['pickup_hour', 'PULocationID']).size().reset_index(name='ride_count')

            df_weather = pd.read_csv(weather_csv_path)
            df_weather['datetime'] = pd.to_datetime(df_weather['datetime']).dt.tz_localize('America/New_York', nonexistent='shift_forward')

            start_date = df_weather['datetime'].min()
            end_date = df_weather['datetime'].max()
            agg_taxi = agg_taxi[(agg_taxi['pickup_hour'] >= start_date) & (agg_taxi['pickup_hour'] <= end_date)]

            merged_df = pd.merge(
                agg_taxi,
                df_weather,
                left_on='pickup_hour',
                right_on='datetime',
                how='left'
            ).drop(columns=['datetime'])

            logging.info(f"Merged aggregated data shape: {merged_df.shape}")
            return merged_df
        except Exception as e:
            raise TaxiDemandException(f"Failed merging datasets: {e}", sys)

    def add_temporal_features(self, df):
        try:
            df['pickup_hour'] = pd.to_datetime(df['pickup_hour'], utc=True).dt.tz_convert('America/New_York')
            df['hour'] = df['pickup_hour'].dt.hour
            df['day_of_week'] = df['pickup_hour'].dt.dayofweek
            df['month'] = df['pickup_hour'].dt.month
            df['is_rain'] = (df['precipitation'] > 0).astype(int)
            logging.info("Added temporal and weather features")
            return df
        except Exception as e:
            raise TaxiDemandException(f"Failed adding features: {e}", sys)

    def split_and_save_data(self, df):
        try:
            logging.info("Splitting data into train and test sets")
            train_set, test_set = train_test_split(
                df,
                test_size=self.data_ingestion_config.train_test_split_ratio,
                random_state=42
            )
            os.makedirs(self.data_ingestion_config.data_ingestion_ingested_dir, exist_ok=True)

            # Reorder columns - move 'ride_count' to the last position
            def move_ride_count_to_end(df_in):
                cols = list(df_in.columns)
                if 'ride_count' in cols:
                    cols.append(cols.pop(cols.index('ride_count')))
                return df_in[cols]

            train_set = move_ride_count_to_end(train_set)
            test_set = move_ride_count_to_end(test_set)

            train_path = self.data_ingestion_config.training_file_path
            test_path = self.data_ingestion_config.testing_file_path

            train_set.to_csv(train_path, index=False)
            test_set.to_csv(test_path, index=False)
            logging.info(f"Saved train data to {train_path}")
            logging.info(f"Saved test data to {test_path}")

            return DataIngestionArtifact(train_file_path=train_path, test_file_path=test_path)
        except Exception as e:
            raise TaxiDemandException(f"Failed splitting/saving data: {e}", sys)


    def initiate_data_ingestion(self):
        try:
            logging.info("Starting data ingestion workflow")

            weather_path = self.fetch_weather_data()
            trip_files = self.fetch_tlc_trip_data()

            merged_df = self.load_and_merge_datasets(trip_files, weather_path)
            df_with_features = self.add_temporal_features(merged_df)
            artifact = self.split_and_save_data(df_with_features)

            logging.info("Data ingestion workflow completed successfully")
            return artifact
        except Exception as e:
            raise TaxiDemandException(f"Error during data ingestion: {e}", sys)
