import csv
import os
import re
import logging
import getpass
import requests
import ipaddress
import datetime

BOUNCE_FILE_PATH = "/Users/vikkushw/Scripts/Secure Email"
BOUNCE_LOGS_DIR = ""

logging.basicConfig(filename=BOUNCE_FILE_PATH+"/esa_script_logs.log",
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    filemode='w')

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
consoleHandler = logging.StreamHandler()
logger.addHandler(consoleHandler)

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

KEY7 = " - "
KEY8 = " \("

re_patterns = [rf"(?<={KEY7})(.*?)(?={KEY8})", r"\('(.*?)\)"]

HEADERS_JSON = {"Content-Type":"application/json",
            "Accept": "application/json"}

HEADERS_TEXT = {"Content-Type":"text/plain"}

OUT_FILE = os.path.join(BOUNCE_FILE_PATH, 'consolidated_bounced_logs_output.csv')

columns = ['Message MID', 'Hostname' ,'Message Status', 'Sender IP', 'Recipient',
            'Subject', 'Timestamp', 'Sender', 'BounceReason']

def extract_data(line, patterns):
    matched_data = []
    for pattern in patterns:
        match = re.search(pattern, line)
        if match:
            matched_data.append(str(match.group()).strip())
    return matched_data

def search_mid_in_logs(directory, keyword, recipient):
    for filename in os.listdir(directory):
        filepath = os.path.join(directory, filename)
        if os.path.isfile(filepath):
            try:
                with open(filepath, 'r', encoding='utf-8') as text_file:
                    for line in text_file:
                        if "Bounced:" in line:
                            if keyword in line:
                                if recipient in line:
                                    return line.strip()
            except Exception as e:
                print(f"Error reading {filename}: {e}")
    return None

def write_data(json_data):
    extracted_data = []
    with open(OUT_FILE, 'a', newline='', encoding='utf-8') as outfile:
        writer = csv.writer(outfile)
        for attributes in json_data["data"]:
            recipient_list = []
            recipient_emails = []
            message_mid = attributes["attributes"]["mid"][0]
            bounce_mid = attributes["attributes"]["mid"][1]
            for recipient in attributes["attributes"]["recipient"]:
                matched_line = search_mid_in_logs(BOUNCE_LOGS_DIR, str(message_mid), recipient)
                if matched_line is not None:
                    recipient_list.append(matched_line)
                    recipient_emails.append(recipient)
            for recipient_line, email in zip(recipient_list, recipient_emails):
                reason = ""
                if recipient_line is not None:
                    reason = extract_data(recipient_line, re_patterns)
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

while True:
    SMA_URL = f"http://{SMA}:6080/sma/api/v2.0/message-tracking/messages?startDate={start_date}.000Z&endDate={end_date}.000Z&ciscoHost=All_Hosts&searchOption=messages&offset={OFFSET}&limit={LIMIT}&hardBounced=True"
    try:
        response = requests.get(SMA_URL, headers=HEADERS_JSON,
                                timeout=100, auth=(user_sma, pwd_sma))
    except requests.exceptions.Timeout:
        logger.error("Time out to %s", SMA)
        break
    if response.status_code == 200:
        data = response.json()
        if data["meta"]["totalCount"] == 0:
            break
        OFFSET += 100
        write_data(data)
        print(f"Number of records processed from SMA: {OFFSET}")
    else:
        logger.error("Unable to connect successfully with %s.", SMA)
        break
