import sys
from src.taxi_demand.logging.logger import logging

class TaxiDemandException(Exception):
    """
    Custom exception class for Taxi Demand forecasting application.
    
    This class extends the
    built-in Exception class to provide a more specific exception
    for the Taxi Demand forecasting application. It can be used to
    handle errors related to data processing, model training, and
    prediction tasks.
    """
    def __init__(self, error_message, error_details:sys):
        self.error_message = error_message
        _, _, exc_tb = error_details.exc_info()

        self.lineno = exc_tb.tb_lineno
        self.filename = exc_tb.tb_frame.f_code.co_filename

    def __str__(self):
        return "Error occurred in python script name [{0}] line number [{1}] error message [{2}]".format(
            self.filename, self.lineno, self.error_message
        )