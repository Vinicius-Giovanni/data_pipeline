import logging
import os
from config.settings import LOG_PATH
from logging.handlers import RotatingFileHandler

def setup_logging(level=logging.INFO):

    log_dir = os.path.dirname(LOG_PATH)
    os.makedirs(log_dir, exist_ok=True)

    logger = logging.getLogger('pipeline_logger')
    logger.setLevel(level)

    formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

    file_handler = RotatingFileHandler(LOG_PATH, maxBytes=10*1024*1024, backupCount=5)
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    if not logger.handlers:
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
    
    return logger
