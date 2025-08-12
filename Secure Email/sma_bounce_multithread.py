import csv
import time
import os
import re
import logging
import getpass
import requests
import ipaddress
import datetime
import threading
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor, as_completed

# Total maximum number of threads for bounce log files search
MAX_WORKERS = 6
results_lock = threading.Lock()
found_matched_line = ''

def extract_data(line, patterns):
    matched_data = []
    for pattern in patterns:
        match = re.search(pattern, line)
        if match:
            matched_data.append(str(match.group()).strip())
    return matched_data

def search_file(filepath, keyword, recipient):
    """
    Searches for a given term in a single file.
    Returns a list of (line_number, line_content) tuples if found, otherwise an empty list.
    """
    try:
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            for line_num, line in enumerate(f, 1):
                if "Bounced:" in line:
                    if keyword in line:
                        if recipient in line:
                            return line.strip()
    except Exception as e:
        print(f"Error reading file {filepath}: {e}")
        return None

def write_data(output_file, directory, json_data, sender_email, receiver_email, re_patterns):
    
    global found_matched_line
    
    extracted_data = []
    with open(output_file, 'a', newline='', encoding='utf-8') as outfile:
        writer = csv.writer(outfile)
        for attributes in json_data["data"]:
            recipient_list = []
            recipient_emails = []
            message_mid = attributes["attributes"]["mid"][0]
            with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                future_to_filepath = None
                for recipient in attributes["attributes"]["recipient"]:
                    for filename in os.listdir(directory):
                        filepath = os.path.join(directory, filename)
                        if os.path.isfile(filepath):
                                # Submit tasks and store Future objects
                                future_to_filepath = executor.submit(search_file, filepath, str(message_mid), recipient)

                                # Process results as they complete
                    for future in concurrent.futures.as_completed([future_to_filepath]):
                        try:
                            matches = future.result()
                            print(matches)
                            if matches:
                                with results_lock: # Acquire lock before modifying shared list
                                    found_matched_line = matches
                                    print(found_matched_line)
                        except Exception as exc:
                            print(f"'{filepath}' generated an exception: {exc}")
                            
                    if found_matched_line != '':
                        recipient_list.append(found_matched_line)
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

def main():
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
        sender_email_input = input("Please enter sender email address. Press ENTER if you want to search for any sender email:")
        if (re.fullmatch(email_regex, sender_email_input) or sender_email_input == ''):
            break
        print("Please enter valid email address or press just ENTER to leave it blank")

    while True:
        receiver_email_input = input("Please enter receiver email address. Press ENTER if you want to search for any receiver email:")
        if (re.fullmatch(email_regex, receiver_email_input) or receiver_email_input == ''):
            break
        print("Please enter valid email address or press just ENTER to leave it blank")

    KEY7 = r" - "
    KEY8 = r" \("

    re_patterns = [rf"(?<={KEY7})(.*?)(?={KEY8})", r"\((.*?)\)"]

    HEADERS_JSON = {"Content-Type":"application/json",
                "Accept": "application/json"}

    OUT_FILE = os.path.join(BOUNCE_FILE_PATH, 'consolidated_bounced_logs_output.csv')

    columns = ['Message MID', 'Hostname' ,'Message Status', 'Sender IP', 'Recipient',
                'Subject', 'Timestamp', 'Sender', 'BounceReason']

    OFFSET = 0
    LIMIT = 100
    logger.info("Connecting to SMA: %s", SMA)
    with open(OUT_FILE, 'a', newline='', encoding='utf-8') as outfile:
        col_writer = csv.writer(outfile)
        col_writer.writerow(columns)

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
            write_data(OUT_FILE, BOUNCE_LOGS_DIR, data, sender_email_input, receiver_email_input, re_patterns)
            print(f"Total Number of records processed from SMA: {OFFSET}.")
        else:
            logger.error("Unable to connect successfully with %s.", SMA)

    end_time = time.time()
    print(f"Search completed in {end_time - start_time:.2f} seconds.")
    print(f"Results saved to {OUT_FILE}")
    
if __name__ == "__main__":
    main()
