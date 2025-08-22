from src.taxi_demand.components.data_ingestion import DataIngestion


from src.taxi_demand.exception.exception import TaxiDemandException
from src.taxi_demand.logging.logger import logging
from src.taxi_demand.entity.config_entity import TrainingPipelineConfig, DataIngestionConfig

import sys

if __name__ == "__main__":
    try:
        training_pipeline_config = TrainingPipelineConfig()

        # Data Ingestion
        data_ingestion_config = DataIngestionConfig(training_pipeline_config=training_pipeline_config)
        data_ingestion = DataIngestion(data_ingestion_config=data_ingestion_config)
        logging.info(f"Data Ingestion Config: {data_ingestion_config}")
        data_ingestion_artifact = data_ingestion.initiate_data_ingestion()
        logging.info("Data Ingestion completed successfully.")
    except Exception as e:
        raise TaxiDemandException(e, sys)