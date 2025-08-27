from src.taxi_demand.entity.artifact_entity import RegressionMetricArtifact
from src.taxi_demand.exception.exception import TaxiDemandException
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import numpy as np
import sys

def get_regression_score(y_true, y_pred):
    try:
        mae = mean_absolute_error(y_true, y_pred)
        mse = mean_squared_error(y_true, y_pred)
        rmse = np.sqrt(mse)
        r2 = r2_score(y_true, y_pred)
        regression_metric = RegressionMetricArtifact(mae==mae, mse=mse, rmse=rmse, r2=r2)
        return regression_metric
    except Exception as e:
        raise TaxiDemandException(e, sys)