import logging
import os
from utils.constants import PROJECT_DIRECTORY, LOG_CATALOG


def configure_logger(logger_name: str, file_name: str) -> logging.Logger:
    formatter = logging.Formatter(fmt='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
                                  datefmt='%m/%d/%Y %I:%M:%S %p')

    logs_directory = os.path.join(PROJECT_DIRECTORY, LOG_CATALOG)
    if not os.path.exists(logs_directory):
        os.makedirs(logs_directory)
    handler = logging.FileHandler(filename=os.path.join(logs_directory, file_name), mode='a')
    handler.setFormatter(formatter)
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
    return logger
