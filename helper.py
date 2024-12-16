import os
import sys
import time
import glob
import asyncio

from datetime import datetime
from functools import wraps

LOG_PATH = "./logs/"
LOG_RETAINTION_IN_DAYS = 2

class Logger:
    def __init__(self, filename):
        self.terminal = sys.stdout
        self.log = open(filename, "a")

    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)

    def flush(self):
        self.terminal.flush()
        self.log.flush()

def log_decorator(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        # os.mkdir(LOG_PATH)
        now = datetime.now()
        log_filename = f"./logs/{now.strftime('%d-%m-%H')}-model.log"

        logger = Logger(log_filename)
        sys.stdout = logger
        
        try:
            print(f"Calling {func.__name__}")
            result = await func(*args, **kwargs)
            print(f"Finished {func.__name__}")
            return result
        finally:
            sys.stdout = logger.terminal
            logger.log.close()
    delete_older_logs()
    return wrapper


def delete_older_logs():
    now = time.time()
    age_threshold = LOG_RETAINTION_IN_DAYS * 24 * 60 * 60
    for log_file in glob.glob(os.path.join(LOG_PATH, "*.log")):
        file_mtime = os.path.getmtime(log_file)
        if (now - file_mtime) > age_threshold:
            os.remove(log_file)
            print(f"Deleted {log_file}")
    print("Log cleanup completed.")


def get_latest_log_file():

    file_type = "*.log"

    search_pattern = os.path.join(LOG_PATH, file_type)
    files = glob.glob(search_pattern)
    max_file = None
    if files:
        max_file = max(files, key=os.path.getctime)

    return max_file


async def read_log_file(file_path):
    with open(file_path, "r") as f:
        while True:
            line = f.readline().strip()
            if line:
                if not line.startswith(": ping"):
                    yield line
            else:
                await asyncio.sleep(1)