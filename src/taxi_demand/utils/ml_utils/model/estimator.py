from src.taxi_demand.exception.exception import TaxiDemandException
import sys

class RegressionModel:
    def __init__(self, preprocessor, scaker, model):
        try:
            self.preprocessor = preprocessor
            self.model = model
        except Exception as e:
            raise TaxiDemandException(e, sys)
        
    def predict(self, X):
        try:
            # Apply preprocessing pipeline to raw dataframe
            X_transformed = self.preprocessor.transform(X)

            # Keras model expects numpy input
            preds = self.model.predict(X_transformed)
            return preds
        except Exception as e:
            raise TaxiDemandException(e, sys)