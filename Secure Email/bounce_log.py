import csv
import os
import re
import logging
import getpass
import requests

BOUNCE_FILE_PATH = "/Users/vikkushw/Scripts/Secure Email"

logging.basicConfig(filename=BOUNCE_FILE_PATH+"/esa_script_logs.log",
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    filemode='w')

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
consoleHandler = logging.StreamHandler()
logger.addHandler(consoleHandler)

user = input("Please enter the Username of ESA: ")
pwd = getpass.getpass(prompt="Enter the Password: ")
start_date = input("Please enter start date and time in the format YYYY-MM-DDTHH:MM:SS:")
end_date = input("Please enter end date and time in the format YYYY-MM-DDTHH:MM:SS:")

ESA_HOSTS_FILE_PATH = BOUNCE_FILE_PATH+"/esa.csv"
HEADERS_JSON = {"Content-Type":"application/json",
            "Accept": "application/json"}

HEADERS_TEXT = {"Content-Type":"text/plain"}

TEMP_FILE = BOUNCE_FILE_PATH+"/temp_log_file.txt"
OUT_FILE = os.path.join(BOUNCE_FILE_PATH, 'consolidated_esa_output.csv')

KEY1 = "Info:"
KEY2 = ": DCID"
KEY3 = "MID"
KEY4 = "From:<"
KEY5 = "> To:<"
KEY6 = "> RID"
KEY7 = " - "
KEY8 = " \("

columns = ['Timestamp', 'ESA IP' ,'Info', 'MID', 'From', 'To', 'Reason', 'Description']
re_patterns = [rf"(.*?)(?={KEY1})", rf"(?<={KEY1})(.*?)(?={KEY2})", rf"(?<={KEY3})(.*?)(?={KEY4})",
            rf"(?<={KEY4})(.*?)(?={KEY5})", rf"(?<={KEY5})(.*?)(?={KEY6})",
            rf"(?<={KEY7})(.*?)(?={KEY8})", r"\['(.*?)\']"]

def extract_data(line, patterns):
    matched_data = []
    for pattern in patterns:
        match = re.search(pattern, line)
        if match:
            matched_data.append(str(match.group()).strip())
    return matched_data

def download_log_file(esa_ip, url):
    download_url = f"http://{esa_ip}:6080{url}"
    try:
        logger.info("Downloading bounce log file %s from ESA: %s", url, esa_ip)
        file_response = requests.get(download_url, headers=HEADERS_TEXT,
                                        timeout=100, auth=(user, pwd))
    except requests.exceptions.Timeout:
        logger.error("Time out to %s", esa_ip)
        return False
    else:
        if file_response.status_code == 200:
            with open(TEMP_FILE, "w") as temp_file:
                temp_file.write(file_response.text)
                logger.info("Logs downloaded from bounce log file %s. Processing further ...",
                                download_url)
            return True
        else:
            logger.error("Unable to connect with %s and download log file. Continuing...",
                            esa_ip)
            return False

with open(OUT_FILE, 'w') as out_file:
    writer = csv.writer(out_file)
    writer.writerow(columns)
    with open(ESA_HOSTS_FILE_PATH, 'r') as esa_host:
        reader = csv.reader(esa_host)
        for esa in reader:
            URL = f"http://{esa[0]}:6080/esa/api/v2.0/logs/bounces/?startDate={start_date}.000Z&endDate={end_date}.000Z"
            DOWNLOAD_URL = ""
            try:
                logger.info("Connecting to ESA: %s", esa[0])
                response = requests.get(URL, headers=HEADERS_JSON, timeout=10, auth=(user, pwd))
            except requests.exceptions.Timeout:
                logger.error("Time out to %s", esa[0])
            if response.status_code == 200:
                data = response.json()
            else:
                logger.error("Unable to connect successfully with %s. Continuing with next one",
                            esa[0])
                continue
            for file_url in data["data"]:
                file_download_status = download_log_file(esa[0], file_url["downloadUrl"])
                if file_download_status is True:
                    with open(TEMP_FILE, 'r') as in_file:
                        for text_line in in_file:
                            extracted_data = extract_data(text_line.strip(), re_patterns)
                            if len(extracted_data) > 2 and extracted_data[1] == "Bounced":
                                extracted_data.insert(1, esa[0])
                                writer.writerow(extracted_data)
                else:
                    continue

    logger.info("Deleting temporary file %s", TEMP_FILE)
    os.remove(TEMP_FILE)
    logger.info("Consolidate CSV file generated for all the bounce logs from ESAs at %s",
                        OUT_FILE)
