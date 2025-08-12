import csv
import os
import re
import logging
import getpass
import requests
import ipaddress

BOUNCE_FILE_PATH = "/Users/vikkushw/Scripts/Secure Email"

MID = input("Enter MID: ")

KEY7 = " - "
KEY8 = " \("

re_patterns = [rf"(?<={KEY7})(.*?)(?={KEY8})", r"\('(.*?)\)"]

def extract_data(line, patterns):
    matched_data = []
    for pattern in patterns:
        match = re.search(pattern, line)
        if match:
            matched_data.append(str(match.group()).strip())
    return matched_data

def search_mid_in_logs(directory, keyword):
    for filename in os.listdir(directory):
        filepath = os.path.join(directory, filename)
        if os.path.isfile(filepath) and filename.endswith(".text"):
            try:
                with open(filepath, 'r', encoding='utf-8') as text_file:
                    print(f"\n---- Searching in: {filename} ---")
                    line_number = 0
                    for line in text_file:
                        line_number += 1
                        if keyword in line:
                            print(f"Line {line_number}: {line.strip()}")
                            return line.strip()
            except Exception as e:
                print(f"Error reading {filename}: {e}")

matched_line = search_mid_in_logs(BOUNCE_FILE_PATH, str(MID))
data = extract_data(matched_line, re_patterns)

print(data)
