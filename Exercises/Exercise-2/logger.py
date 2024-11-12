import logging

level = logging.DEBUG
logging_format = "[%(levelname)s] %(asctime)s - %(message)s"
logging.basicConfig(level = level, format=logging_format)

def log_debug(message:str):
    logging.debug(message)
def log_info(message:str):
    logging.info(message)
def log_error(message:str):
    logging.error(message)