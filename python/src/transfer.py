#!/usr/bin/python3
# Python 3.11
import logging
import os
import sys
import time


def main():
    while True:
        time.sleep(1)
        logging.info("Running")


def init_logging():
    log_format = f"%(asctime)s [%(processName)s] [%(name)s] [%(levelname)s] %(message)s"
    # logging.getLogger('').disabled = True
    log_level = logging.DEBUG
    # noinspection PyArgumentList
    logging.basicConfig(
        format=log_format,
        level=log_level,
        force=True,
        handlers=[
            logging.FileHandler(filename=app_dir('debug.log'), mode='w', encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )


def app_dir(relative_path='') -> os.path:
    if getattr(sys, 'frozen', False):
        path = os.path.dirname(sys.executable)
    else:  # called from pycharm or direct with python
        path = os.path.dirname(__file__)
    return os.path.join(path, relative_path)


def home_dir(relative_path='') -> os.path:
    if getattr(sys, 'frozen', False):
        path = sys._MEIPASS
    else:  # called from pycharm or direct with python
        path = os.path.dirname(__file__)
    return os.path.join(path, relative_path)


if __name__ == "__main__":
    init_logging()
    main()
