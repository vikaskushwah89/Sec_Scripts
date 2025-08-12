import os
import csv
import re
import datetime

start_date = input("Please enter start date and time in the format DD-MM-YYYY HH:MM:SS:")
end_date = input("Please enter end date and time in the format DD-MM-YYYY HH:MM:SS:")

BOUNCE_FILE_PATH = "/Users/vikkushw/Scripts/Secure Email"
IN_FILE = os.path.join(BOUNCE_FILE_PATH, 'bounces-1.text')
OUT_FILE = os.path.join(BOUNCE_FILE_PATH, 'output.csv')

KEY1 = "Info:"
KEY2 = ": DCID"
KEY3 = "MID"
KEY4 = "From:<"
KEY5 = "> To:<"
KEY6 = "> RID"
KEY7 = " - "
KEY8 = " \("

columns = ['Timestamp', 'Info', 'MID', 'From', 'To', 'Reason', 'Description']
patterns = [rf"(.*?)(?={KEY1})", rf"(?<={KEY1})(.*?)(?={KEY2})", rf"(?<={KEY3})(.*?)(?={KEY4})",
            rf"(?<={KEY4})(.*?)(?={KEY5})", rf"(?<={KEY5})(.*?)(?={KEY6})",
            rf"(?<={KEY7})(.*?)(?={KEY8})", r"\[(.*?)\]"]

def extract_data(line, patterns):
    matched_data = []
    for pattern in patterns:
        match = re.search(pattern, line)
        if match:
            matched_data.append(str(match.group()).strip())
    return matched_data

try:
    start_d = datetime.datetime.strptime(start_date, "%d-%m-%Y %H:%M:%S")
    end_d = datetime.datetime.strptime(end_date, "%d-%m-%Y %H:%M:%S")
except ValueError:
    print("Please enter the date and time in the correct format (DD-MM-YYYY HH:MM:SS)")
else:
    with open(OUT_FILE, 'w') as out_file:
        writer = csv.writer(out_file)
        writer.writerow(columns)
        with open(IN_FILE, 'r') as in_file:
            for line in in_file:
                extracted_data = extract_data(line.strip(), patterns)
                log_date = datetime.datetime.strptime(extracted_data[0], "%a %b %d %H:%M:%S %Y")
                if log_date >= start_d and log_date <= end_d:
                    writer.writerow(extracted_data)
