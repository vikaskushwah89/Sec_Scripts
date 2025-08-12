import csv
import time
import os
import re
import logging
import getpass
import requests
import ipaddress
import datetime
import pandas as pd
#from rich.progress import Progress

BOUNCE_FILE_PATH = r"D:\SMA_API_Scripts"
BOUNCE_LOGS_CSV_DIR = r"D:\SMA_API_Scripts\ESABounceLogsCSV"

consolidated_csv = os.path.join(BOUNCE_LOGS_CSV_DIR, "consolidated_bounced_logs.csv")

logging.basicConfig(filename=BOUNCE_FILE_PATH+"/esa_script_logs.log",
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    filemode='w')

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

while True:
    SMA = input("Please enter SMA IP or FQDN: ")
    try:
        ipaddress.ip_address(SMA)
        break
    except ValueError:
        print("Invalid SMA IP address. Please enter correct IP")

user_sma = input("Please enter the Username of SMA: ")
pwd_sma = getpass.getpass(prompt="Enter SMA Password: ")

while True:
    start_date = input("Please enter start date and time in the format YYYY-MM-DDTHH:MM:SS:")
    try:
        date_object_user = datetime.datetime.strptime(start_date, "%Y-%m-%dT%H:%M:%S")
        break
    except ValueError:
        print("Please enter the date and time in the correct format (YYYY-MM-DDTHH:MM:SS)")

while True:
    end_date = input("Please enter end date and time in the format YYYY-MM-DDTHH:MM:SS:")
    try:
        date_object_user = datetime.datetime.strptime(end_date, "%Y-%m-%dT%H:%M:%S")
        break
    except ValueError:
        print("Please enter the date and time in the correct format (YYYY-MM-DDTHH:MM:SS)")

email_regex = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,7}\b'

input_no_of_records  = input("Please enter total number of records to fetch:")
TOTAL_RECORDS = int(input_no_of_records)

while True:
    sender_email = input("Please enter sender email address. Press ENTER if you want to search for any sender email:")
    if (re.fullmatch(email_regex, sender_email) or sender_email==''):
        break
    print("Please enter valid email address or press just ENTER to leave it blank")

while True:
    receiver_email = input("Please enter receiver email address. Press ENTER if you want to search for any receiver email:")
    if (re.fullmatch(email_regex, receiver_email) or receiver_email==''):
        break
    print("Please enter valid email address or press just ENTER to leave it blank")

HEADERS_JSON = {"Content-Type":"application/json",
            "Accept": "application/json"}

OUT_FILE = os.path.join(BOUNCE_FILE_PATH, 'Report_Generator_output.csv')

columns = ['Message MID', 'Hostname' ,'Message Status', 'Sender IP', 'Recipient',
            'Subject', 'Timestamp', 'Sender', 'Error','BounceReason']


def write_data(json_data, row_data):
    extracted_data = []
    with open(OUT_FILE, 'a', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        for attributes in json_data["data"]:
            extracted_data = [row_data["MID"], attributes["attributes"]["hostName"],
                                "Bounced",
                                attributes["attributes"]["senderIp"],
                                row_data["To"],
                                attributes["attributes"]["subject"],
                                attributes["attributes"]["timestamp"],
                                attributes["attributes"]["sender"],
                                row_data["Reason"],
                                row_data["Description"]
                                ]
            if (sender_email != '' and receiver_email != '') and (sender_email == extracted_data[7] or receiver_email == extracted_data[4]):
                writer.writerow(extracted_data)
            elif sender_email == '' and receiver_email == extracted_data[4]:
                writer.writerow(extracted_data)
            elif receiver_email == '' and sender_email == extracted_data[7]:
                writer.writerow(extracted_data)
            elif sender_email == '' and receiver_email == '':
                writer.writerow(extracted_data)

OFFSET = 0
LIMIT = 100
logger.info("Connecting to SMA: %s", SMA)
with open(OUT_FILE, 'w', newline='', encoding='utf-8') as outfile:
    col_writer = csv.writer(outfile)
    col_writer.writerow(columns)

start_time = time.time()

print("Reading Consolidated Bounced Logs into memory...")
df_read = pd.read_csv(consolidated_csv, low_memory=False)
print("File load complete!")

#TOTAL_RECORDS = len(df.index)
#print(f"Processing total number of records: {TOTAL_RECORDS}")

for index, row in df_read.iterrows():
    SMA_URL = f"http://{SMA}:6080/sma/api/v2.0/message-tracking/messages?startDate={start_date}.000Z&endDate={end_date}.000Z&ciscoHost=All_Hosts&searchOption=messages&offset={OFFSET}&limit={LIMIT}&hardBounced=True&ciscoMid={row["MID"]}"
    try:
        response = requests.get(SMA_URL, headers=HEADERS_JSON,
                                timeout=10, auth=(user_sma, pwd_sma))
    except requests.exceptions.Timeout:
        logger.error("Time out to %s", SMA)
    if response.status_code == 200:
        data = response.json()
        write_data(data, row)
        print(f"Total Number of records processed from SMA: {index}.")
    else:
        logger.error("Unable to connect successfully with %s.", SMA)

end_time = time.time()
print(f"Search completed in {end_time - start_time:.2f} seconds.")
print(f"Results saved to {OUT_FILE}")
