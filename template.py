import os
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO, format='[%(asctime)s]: %(message)s:')

project_name = "taxi_demand"

list_of_files = [
    ".github/workflows/main.yaml",
    f"data_schema/schema.yaml",
    f"project_data/sample.txt",
    f"research_notebooks/01. EDA.ipynb",
    f"research_notebooks/02. Model_Training.ipynb",
    f"research_notebooks/Data/sample_data.txt",
    f"src/__init__.py",
    f"src/{project_name}/__init__.py",
    f"src/{project_name}/components/__init__.py",
    f"src/{project_name}/utils/__init__.py",
    f"src/{project_name}/utils/main_utils/__init__.py",
    f"src/{project_name}/utils/main_utils/utils.py",
    f"src/{project_name}/utils/ml_utils/__init__.py",
    f"src/{project_name}/utils/ml_utils/model/__init__.py",
    f"src/{project_name}/utils/ml_utils/model/estimator.py",
    f"src/{project_name}/utils/ml_utils/metric/__init__.py",
    f"src/{project_name}/utils/ml_utils/metric/metric.py",
    f"src/{project_name}/config/__init__.py",
    f"src/{project_name}/config/configuration.py",
    f"src/{project_name}/pipeline/__init__.py",
    f"src/{project_name}/pipeline/training_pipeline.py",
    f"src/{project_name}/entity/__init__.py",
    f"src/{project_name}/entity/config_entity.py",
    f"src/{project_name}/entity/artifact_entity.py",
    f"src/{project_name}/exception/__init__.py",
    f"src/{project_name}/exception/exception.py",
    f"src/{project_name}/logging/__init__.py",
    f"src/{project_name}/logging/logger.py",
    f"src/{project_name}/constants/__init__.py",
    f"src/{project_name}/constants/training_pipeline/__init__.py",
    "main.py",
    "Dockerfile",
    ".gitignore",
    '.dockerignore',
    "setup.py",
    "templates/index.html",
    "app.py",
    "requirements.txt"
]

for filepath in list_of_files:
    filepath = Path(filepath)
    filedir, filename = os.path.split(filepath)
    if filedir != "":
        os.makedirs(filedir, exist_ok=True)
        logging.info(f"Creating directory: {filedir} for file: {filename}")

    if (not os.path.exists(filepath)) or (os.path.getsize(filepath) == 0):
        with open(filepath, "w") as f:
            pass
        logging.info(f"Creating empty file: {filepath}")
    else:
        logging.info(f"{filename} already exists")
