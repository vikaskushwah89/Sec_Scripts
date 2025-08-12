import fmcapi
from pprint import pprint
from fmcapi.api_objects.policy_services.accessrules import Bulk
import csv
import json

host = "FMC IP or FQDN"
username = "username"
password = "password"

input_file = "Input file path" # This is the CSV file which contains Policy Name and disabled Rule Name. DO NOT CHANGE THE FILE HEADERS.
reference_file = "deleted_rules.json" # This is the .json file which stores all rule details (including each attribute) those are deleted by the script.

data_list = []
rules_data = []

# Read the input CSV file to get Policy and Rule Names
with open(input_file, 'r') as file:
    csv_reader = csv.DictReader(file)
    for row in csv_reader:
        data_list.append(row)

# Initiate a connection to the FMC and request tokens
with fmcapi.FMC(
        host=host,
        username=username,
        password=password,
        autodeploy=False,
    ) as fmc1:

    # Get the desired rule from policy and disable it. Also, store rule data into a list
    for item in data_list:
        acprule = fmcapi.AccessRules(fmc=fmc1, acp_name=item["Policy Name"])
        acprule.get(name = item["Rule Name"])
        if acprule.enabled == False:
            rules_data.append(acprule.get(name = item["Rule Name"]))
            acprule.delete()

# Write rule data which is disabled to json file.
json_object = json.dumps(rules_data, indent=4)
with open(reference_file, 'w') as outfile:
    outfile.write(json_object)

print("File " + reference_file + " contains all the disabled rules with attributes for your reference")