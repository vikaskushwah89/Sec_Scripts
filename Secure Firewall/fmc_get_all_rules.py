import csv
import os
import ipaddress
import getpass
import fmcapi

while True:
    FMC_IP = input("Please enter SMA IP or FQDN: ")
    try:
        ipaddress.ip_address(FMC_IP)
        break
    except ValueError:
        print("Invalid SMA IP address. Please enter correct IP")

user_fmc = input("Please enter the Username of SMA: ")
pwd_fmc = getpass.getpass(prompt="Enter SMA Password: ")

# Local files path. Update as per your requirement. This is where files will be created.
LOCAL_PATH = "/Users/vikkushw/Scripts/Secure Firewall/InputFiles"

# Input CSV file that contains Policy names. DO NOT CHANGE HEADERS!
input_file = os.path.join(LOCAL_PATH, "input.csv")

# Script will create this file to write rule data with rule IDs.
temp_rule_data_file = os.path.join(LOCAL_PATH, "rule_data_with_ids.csv")

policy_list = []
rules_data = []

columns = ['Rule Name', 'ID' ,'Policy Name', 'Enabled']

def build_rule_data_file(rules):
    """This function extracts below information from all the rules and writes
    to a csv file.

    Rule name, rule id, policy name and whether the rule is enabled or not (True or False)
    """
    with open(temp_rule_data_file, 'w', encoding='utf-8') as temp_file:
        writer = csv.writer(temp_file)
        writer.writerow(columns)
        for rule_set in rules:
            for items in rule_set["items"]:
                extracted_data = [
                    items["name"],
                    items["id"],
                    items["metadata"]["accessPolicy"]["name"],
                    items["enabled"]
                ]
                writer.writerow(extracted_data)

# Read the input CSV file to get Policy and Rule Names
with open(input_file, 'r', encoding='utf-8') as file:
    csv_reader = csv.DictReader(file)
    for row in csv_reader:
        policy_list.append(row)

# Initiate a connection to the FMC and request tokens
with fmcapi.FMC(
        host=FMC_IP,
        username=user_fmc,
        password=pwd_fmc,
        autodeploy=False,
    ) as fmc1:

    # Get the desired rule from policy and disable it. Also, store rule data into a list
    for item in policy_list:
        acprule = fmcapi.AccessRules(fmc=fmc1, acp_name=item["Policy Name"])
        rules_data.append(acprule.get())
    
    build_rule_data_file(rules_data)

print(f"File {temp_rule_data_file} contains all the rules with IDs for your reference")