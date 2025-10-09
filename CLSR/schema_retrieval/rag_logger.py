import logging
import logging.handlers
import os
from pathlib import Path


class RAGLogger:

    @staticmethod
    def get_logger(logger_name):
        current_file = Path(__file__).resolve()
        current_dir = os.path.dirname(current_file)
        log_file = current_dir + "/server.log"
        logger = logging.getLogger(logger_name)
        logger.setLevel(logging.INFO)
        formatter = logging.Formatter(
            "%(asctime)s - %(funcName)s - %(filename)s:%(lineno)d - %(levelname)s - %(message)s")
        if not logger.handlers:
            file_log_handler = logging.handlers.TimedRotatingFileHandler(filename=log_file, when='d', interval=1, backupCount=3)
            file_log_handler.suffix = "%Y-%m-%d_%H-%M-%S.log"
            file_log_handler.setFormatter(formatter)
            logger.addHandler(file_log_handler)

            ch = logging.StreamHandler()
            ch.setFormatter(formatter)
            logger.addHandler(ch)
        return logger
