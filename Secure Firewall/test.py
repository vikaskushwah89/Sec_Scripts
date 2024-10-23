from collections import defaultdict
import csv
from pprint import pprint

CSV_DATA_DICT = defaultdict(list)

with open('/Users/vikkushw/Scripts/Secure Firewall/input_data.csv', mode ='r') as file:
    csvFile = csv.DictReader(file)
    for row in csvFile:
        CSV_DATA_DICT[row["NAME"]].append({key: row[key] for key in row if key != "NAME"})

pprint(CSV_DATA_DICT)
