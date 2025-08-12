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
from rich.progress import Progress

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

columns = ['Message MID', 'Hostname' ,'Message Status', 'Sender IP', 'Recipient',
            'Subject', 'Timestamp', 'Sender', 'Error','BounceReason']


def write_data(json_data, df_func, final_data):
    extracted_data = []
    matched_extracted_row = None
    with Progress() as p:
        t = p.add_task("Processing...", total=len(json_data))
        while not p.finished:
            for row in json_data:
                for attributes in row:
                    message_mid = int(attributes["attributes"]["mid"][0])
                    for recipient in attributes["attributes"]["recipient"]:
                        matching_rows = df_func[(df_func["MID"] == message_mid) & (df_func["To"] == recipient)]
                        if not matching_rows.empty:
                            matched_extracted_row = matching_rows.iloc[0]
                            extracted_data = [message_mid, attributes["attributes"]["hostName"],
                                                "Bounced",
                                                attributes["attributes"]["senderIp"],
                                                recipient,
                                                attributes["attributes"]["subject"],
                                                attributes["attributes"]["timestamp"],
                                                attributes["attributes"]["sender"],
                                                matched_extracted_row["Reason"],
                                                matched_extracted_row["Description"]
                                                ]
                            if (sender_email != '' and receiver_email != '') and (sender_email == extracted_data[7] or receiver_email == extracted_data[4]):
                                final_data.append(extracted_data)
                            elif sender_email == '' and receiver_email == extracted_data[4]:
                                final_data.append(extracted_data)
                            elif receiver_email == '' and sender_email == extracted_data[7]:
                                final_data.append(extracted_data)
                            elif sender_email == '' and receiver_email == '':
                                final_data.append(extracted_data)
                p.update(t, advance=1)

OFFSET = 0
LIMIT = 100
logger.info("Connecting to SMA: %s", SMA)

start_time = time.time()

#TOTAL_RECORDS = len(df.index)
#print(f"Processing total number of records: {TOTAL_RECORDS}")


print("Collecting hard bounced records from SMA")
SMA_BOUNCED_RECORDS = []
while OFFSET <= TOTAL_RECORDS:
    SMA_URL = f"http://{SMA}:6080/sma/api/v2.0/message-tracking/messages?startDate={start_date}.000Z&endDate={end_date}.000Z&ciscoHost=All_Hosts&searchOption=messages&offset={OFFSET}&limit={LIMIT}&hardBounced=True"
    try:
        response = requests.get(SMA_URL, headers=HEADERS_JSON,
                                timeout=100, auth=(user_sma, pwd_sma))
    except requests.exceptions.Timeout:
        logger.error("Time out to %s", SMA)
    if response.status_code == 200:
        data = response.json()
        if int(data["meta"]["totalCount"]) == 0:
            OFFSET += 1
        else:
            OFFSET += int(data["meta"]["totalCount"])
        SMA_BOUNCED_RECORDS.append(data["data"])
        print(f"Total Number of records collected from SMA: {OFFSET}.")
    else:
        logger.error("Unable to connect successfully with %s.", SMA)

print("Starting consolidated file processing...")
print("Reading Consolidated Bounced Logs into memory...")
df_read = pd.read_csv(consolidated_csv, low_memory=False)
print("File load complete!")

FINAL_DATA = []

write_data(SMA_BOUNCED_RECORDS, df_read, FINAL_DATA)

print("Writing final data into CSV...")
OUT_FILE = os.path.join(BOUNCE_FILE_PATH, 'consolidated_bounced_logs_output.csv')
with open(OUT_FILE, 'w', newline='', encoding='utf-8') as outfile:
    col_writer = csv.writer(outfile)
    col_writer.writerow(columns)
    col_writer.writerows(FINAL_DATA)

end_time = time.time()
print(f"Search completed in {end_time - start_time:.2f} seconds.")
print(f"Results saved to {OUT_FILE}")
