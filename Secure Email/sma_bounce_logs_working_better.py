import csv
import time
import os
import re
import logging
import getpass
import requests
import ipaddress
import datetime

BOUNCE_FILE_PATH = r"D:\SMA_API_Scripts"
BOUNCE_LOGS_DIR = r"D:\SMA_API_Scripts\ESABounceLogs"

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

TOTAL_RECORDS  = input("Please enter total number of records to fetch:")

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

KEY7 = r" - "
KEY8 = r" \("

re_pattern1 = rf"(?<={KEY7})(.*?)(?={KEY8})"
re_pattern2 = r"\((.*?)\)"
compiled_pattern1 = re.compile(re_pattern1)
compiled_pattern2 = re.compile(re_pattern2)
compiled_patterns = [compiled_pattern1, compiled_pattern2]

HEADERS_JSON = {"Content-Type":"application/json",
            "Accept": "application/json"}

HEADERS_TEXT = {"Content-Type":"text/plain"}

OUT_FILE = os.path.join(BOUNCE_FILE_PATH, 'consolidated_bounced_logs_output.csv')

columns = ['Message MID', 'Hostname' ,'Message Status', 'Sender IP', 'Recipient',
            'Subject', 'Timestamp', 'Sender', 'BounceReason']

def get_files_to_search(directory):
    files_list = []
    try:
        for filename in os.listdir(directory):
            filepath = os.path.join(directory, filename)
            if os.path.isfile(filepath):
                files_list.append(filepath)
    except Exception as e:
        print(f"Error reading {filename}: {e}")
    return files_list

def extract_data(line, patterns):
    matched_data = []
    for pattern in patterns:
        match = re.search(pattern, line)
        if match:
            matched_data.append(str(match.group()).strip())
    return matched_data

def search_mid_in_logs(files_to_search_list, keywords):
    for filepath in files_to_search_list:
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as text_file:
            temp_file_list = text_file.readlines()
            for line in temp_file_list:
                if all(keyword in line for keyword in keywords):
                    return line.strip()
    return None

def write_data(file_list, json_data):
    extracted_data = []
    with open(OUT_FILE, 'a', newline='', encoding='utf-8') as outfile:
        writer = csv.writer(outfile)
        for attributes in json_data["data"]:
            recipient_list = []
            recipient_emails = []
            message_mid = attributes["attributes"]["mid"][0]
            for recipient in attributes["attributes"]["recipient"]:
                keywords_to_look = ["Bounced:", str(message_mid), recipient]
                matched_line = search_mid_in_logs(file_list, keywords_to_look)
                if matched_line is not None:
                    recipient_list.append(matched_line)
                    recipient_emails.append(recipient)
            for recipient_line, email in zip(recipient_list, recipient_emails):
                reason = ""
                if recipient_line is not None:
                    reason = extract_data(recipient_line, compiled_patterns)
                extracted_data = [message_mid, attributes["attributes"]["hostName"],
                                    "Bounced",
                                    attributes["attributes"]["senderIp"],
                                    email,
                                    attributes["attributes"]["subject"],
                                    attributes["attributes"]["timestamp"],
                                    attributes["attributes"]["sender"], reason]
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
with open(OUT_FILE, 'a', newline='', encoding='utf-8') as outfile:
    col_writer = csv.writer(outfile)
    col_writer.writerow(columns)

list_of_files = get_files_to_search(BOUNCE_LOGS_DIR)

start_time = time.time()

while OFFSET <= int(TOTAL_RECORDS):
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
        print(f"Received {OFFSET} records from SMA. Processing bounce log lookups...")
        write_data(list_of_files, data)
        print(f"Total Number of records processed from SMA: {OFFSET}.")
    else:
        logger.error("Unable to connect successfully with %s.", SMA)

end_time = time.time()
print(f"Search completed in {end_time - start_time:.2f} seconds.")
print(f"Results saved to {OUT_FILE}")
