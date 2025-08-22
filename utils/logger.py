# === utils/logger.py ===
import logging
import os
from datetime import datetime

LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

# You can customize the log file name
log_filename = f"{LOG_DIR}/app_{datetime.now().strftime('%Y-%m-%d')}.log"

def get_logger(name):
    logger = logging.getLogger(name)

    if not logger.handlers:
        logger.setLevel(logging.INFO)

        # Console logger
        stream_handler = logging.StreamHandler()
        stream_formatter = logging.Formatter('[%(asctime)s] %(levelname)s - %(name)s: %(message)s')
        stream_handler.setFormatter(stream_formatter)
        logger.addHandler(stream_handler)

        # File logger
        file_handler = logging.FileHandler(log_filename)
        file_formatter = logging.Formatter('[%(asctime)s] %(levelname)s - %(name)s: %(message)s')
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)

    return logger
