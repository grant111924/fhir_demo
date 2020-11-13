import logging
from datetime import datetime as dt
import os


def create_logger():
    now = dt.now().strftime("%Y-%m-%d-%H-%M-%S")
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(levelname)s %(message)s',
                        datefmt='%Y-%m-%d %H:%M',
                        handlers=[logging.FileHandler('error_'+now+'.log', 'w', 'utf-8')])
    return  logging.getLogger()