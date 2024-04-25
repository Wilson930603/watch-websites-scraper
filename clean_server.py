import psutil
import datetime
import signal
import time
import os
import shutil
import logging

# variables
process_cutoff_days = 2
log_cutoff_days = 7

# Set logs directory to look in
logs_path = './log'

# Set up logging to file
logging.basicConfig(filename='./log/clean_server.txt', level=logging.DEBUG)

now = datetime.datetime.now()
logging.info(f'START CLEAN PROCESS: {now.strftime("%d-%m-%Y %H:%M:%S")}')

# Calculate cutoff time
cutoff = now - datetime.timedelta(days=log_cutoff_days)

# Loop through directory contents
for filename in os.listdir(logs_path):
    file_path = os.path.join(logs_path, filename)

    # Get last modification time
    mod_time = datetime.datetime.fromtimestamp(os.path.getmtime(file_path))

    # Check if older than cutoff
    if mod_time < cutoff:
        logging.info(f'Deleting old file: {file_path}')

        # Delete file
        if os.path.isdir(file_path):
            shutil.rmtree(file_path)
        else:
            os.remove(file_path)


for proc in psutil.process_iter(['pid', 'create_time', 'cmdline']):
    try:
        p_info = proc.as_dict(attrs=['pid', 'create_time', 'cmdline'])
        age = now - datetime.datetime.fromtimestamp(p_info['create_time'])
        if age.days > 2:
            cmd = ' '.join(p_info['cmdline'])
            if 'scrapy crawl' in cmd:
                logging.info(f"Closing process {proc.pid}, {cmd}")
                try:
                    proc.send_signal(signal.SIGTERM)
                    for i in range(6):
                        time.sleep(10)  # Wait for 10 seconds before checking the status
                        if proc.status() == psutil.STATUS_ZOMBIE:
                            break  # If the process is a zombie, exit the loop and kill the process
                    for _ in range(4):
                        proc.send_signal(signal.SIGTERM)
                        time.sleep(0.5)
                    proc.kill()
                except psutil.NoSuchProcess:
                    logging.info(f"Process {proc.pid} already closed.")
    except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
        pass
