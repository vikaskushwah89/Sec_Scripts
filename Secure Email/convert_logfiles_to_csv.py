import csv
import os
import re
import datetime
import logging
from rich.progress import Progress

LOG_FILE = r"D:\SMA_API_Scripts"
BOUNCE_FILE_PATH = r"D:\SMA_API_Scripts\ESABounceLogs"
BOUNCE_FILE_PATH_CSV = r"D:\SMA_API_Scripts\ESABounceLogsCSV"

out_file = os.path.join(BOUNCE_FILE_PATH_CSV, "consolidated_bounced_logs.csv")

logging.basicConfig(filename=LOG_FILE+"/conversion_script_logs.log",
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    filemode='w')

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
consoleHandler = logging.StreamHandler()
logger.addHandler(consoleHandler)

while True:
    start_date = input("Please enter the start date and time for which you want consolidated report in the format YYYY-MM-DDTHH:MM:SS:")
    try:
        date_object_user_start = datetime.datetime.strptime(start_date, "%Y-%m-%dT%H:%M:%S")
        print(date_object_user_start)
        break
    except ValueError:
        print("Please enter the date and time in the correct format (YYYY-MM-DDTHH:MM:SS)")

while True:
    end_date = input("Please enter the end date and time for which you want consolidated report in the format YYYY-MM-DDTHH:MM:SS:")
    try:
        date_object_user_end = datetime.datetime.strptime(end_date, "%Y-%m-%dT%H:%M:%S")
        print(date_object_user_end)
        break
    except ValueError:
        print("Please enter the date and time in the correct format (YYYY-MM-DDTHH:MM:SS)")

KEY1 = "Info:"
KEY2 = ": DCID"
KEY3 = "MID"
KEY4 = "From:<"
KEY5 = "> To:<"
KEY6 = "> RID"
KEY7 = " - "
KEY8 = " \("

columns = ['Timestamp','Info', 'MID', 'From', 'To', 'Reason', 'Description']
re_patterns = [rf"(.*?)(?={KEY1})", rf"(?<={KEY1})(.*?)(?={KEY2})", rf"(?<={KEY3})(.*?)(?={KEY4})",
            rf"(?<={KEY4})(.*?)(?={KEY5})", rf"(?<={KEY5})(.*?)(?={KEY6})",
            rf"(?<={KEY7})(.*?)(?={KEY8})", r"\((.*?)\)"]

def extract_data(line, patterns):
    matched_data = []
    for pattern in patterns:
        match = re.search(pattern, line)
        if match:
            matched_data.append(str(match.group()).strip())
    return matched_data

with open(out_file, 'w', newline='', encoding='utf-8') as csv_file:
    writer = csv.writer(csv_file)
    writer.writerow(columns)
with Progress() as p:
    t = p.add_task("Processing...", total=len(os.listdir(BOUNCE_FILE_PATH)))
    while not p.finished:
        for filename in os.listdir(BOUNCE_FILE_PATH):
            filepath = os.path.join(BOUNCE_FILE_PATH, filename)
            if os.path.isfile(filepath):
                print(f"Converting {filename}")
                with open(out_file, 'a', newline='', encoding='utf-8') as file:
                    writer = csv.writer(file)
                    with open(filepath, 'r', encoding='utf-8', errors='ignore') as in_file:
                        for text_line in in_file:
                            extracted_data = extract_data(text_line.strip(), re_patterns)
                            if len(extracted_data) > 2 and extracted_data[1] == "Bounced":
                                try:
                                    date_object_timestamp = datetime.datetime.strptime(extracted_data[0], "%a %b %d %H:%M:%S %Y")
                                except ValueError:
                                    print("Date format mismatch. Still taking record into consideration.")
                                if (date_object_timestamp >= date_object_user_start) and (date_object_timestamp <= date_object_user_end):
                                    writer.writerow(extracted_data)
            p.update(t, advance=1)

logger.info("Consolidate CSV file generated for all the bounce logs from ESAs at %s",
                        out_file)
