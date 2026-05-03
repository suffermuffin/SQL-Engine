import os
import sys
import subprocess
import logging

# Creds to https://github.com/lerocha
CHINOOK_URL = "https://github.com/lerocha/chinook-database/raw/refs/heads/master/ChinookDatabase/DataSources/Chinook_Sqlite.sqlite"


def download_file(url : str, save_path : str):
    """ Downloads file using wget """

    file_name = os.path.basename(save_path)
    dir_path  = save_path.removesuffix(file_name)

    subprocess.run(["wget", "-q", "--show-progress", "-P", dir_path, "-O", save_path, url])


def format_logging(level : str = "INFO"):
    
    root_logger = logging.getLogger()
    root_logger.setLevel(level.upper())
    
    while root_logger.handlers:
        handler = root_logger.handlers.pop()
        handler.close()
    
    console_handler = logging.StreamHandler(sys.stdout)
    
    formatter = logging.Formatter(
        fmt="%(asctime)s | \033[1m%(levelname)-8s\033[0m | \033[36m%(name)-25s\033[0m | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)
