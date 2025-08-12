import csv
import os
import json
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

# Script will create this file to write rule data with rule IDs.
temp_rule_data_file = os.path.join(LOCAL_PATH, "rule_data_with_ids.csv")

# This is the .json file which stores all rule details (including each attribute) those are deleted by the script.
reference_file = os.path.join(LOCAL_PATH, "deleted_rules.json")

data_list = []
rules_data = []

# Read the input CSV file to get Policy and Rule Names
with open(temp_rule_data_file, 'r', encoding='utf-8') as file:
    csv_reader = csv.DictReader(file)
    for row in csv_reader:
        data_list.append(row)

# Initiate a connection to the FMC and request tokens
with fmcapi.FMC(
        host=FMC_IP,
        username=user_fmc,
        password=pwd_fmc,
        autodeploy=False,
    ) as fmc1:

    # Get the desired rule from policy and disable it. Also, store rule data into a list
    for item in data_list:
        acprule = fmcapi.AccessRules(fmc=fmc1, acp_name=item["Policy Name"])
        acprule.get(id = item["ID"])
        if acprule.enabled == False:
            rules_data.append(acprule.get(id = item["ID"]))
            acprule.delete()

# Write rule data which is disabled to json file.
json_object = json.dumps(rules_data, indent=4)
with open(reference_file, 'w', encoding='utf-8') as outfile:
    outfile.write(json_object)

print(f"File {reference_file} contains all the rules with IDs for your reference")
