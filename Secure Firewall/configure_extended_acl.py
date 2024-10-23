from collections import defaultdict
import ipaddress
import csv
import fmcapi

HOST = "10.127.245.194"
USERNAME = "vicky"
PASSWORD = "V!cky!123"
EXTENDED_ACL_NAMES = defaultdict()

def translate_netmask_cidr(ip_address, netmask):
    ip4 = ipaddress.IPv4Network((ip_address,netmask))
    return ip4.with_prefixlen

def process_csv_data(file_path):
    csv_data_dict = defaultdict(list)

    with open(file_path, mode ='r', encoding="utf-8") as file:
        csvfile = csv.DictReader(file)
        for row in csvfile:
            csv_data_dict[row["NAME"]].append({key: row[key] for key in row if key != "NAME"})

    return csv_data_dict

def process_ace(fmc_object,
                name,
                action,
                protocol,
                source_type,
                source,
                smask,
                dest_type,
                destination,
                dmask
                ):

    extended_ace = fmcapi.ExtendedAccessListAce()

    if action == "permit" and protocol == "ip":
        if source_type == "network":
            extended_ace.sourceNetworksLiterals = [{"type": "Network",
                                                    "value": translate_netmask_cidr(source, smask)}]
        if source_type == "host":
            extended_ace.sourceNetworksLiterals = [{"type": "Host", "value": source + "/32"}]
        if source_type == "object":
            try:
                extended_ace.sourceNetworksObjects = [{
                            "id": fmcapi.NetworkGroups(fmc=fmc_object, name=source).get()["id"]}]
            except KeyError:
                print(f"No Object with name: {source} found in the FMC")
                return None
        if dest_type == "network":
            extended_ace.destinationNetworksLiterals = [{"type": "Network",
                                "value": translate_netmask_cidr(destination, dmask)}]
        if dest_type == "host":
            extended_ace.destinationNetworksLiterals = [{"type": "Host",
                                                        "value": destination + "/32"}]
        if dest_type == "object":
            try:
                extended_ace.destinationNetworksObjects = [{
                                "id": fmcapi.NetworkGroups(fmc=fmc_object, name=destination).get()["id"]}]
            except KeyError:
                print(f"No Object with name: {destination} found in the FMC")
                return None

        return extended_ace.build_ace()

    print(f"Non IP based ACL entry found for ACL {name}. Skipping Line")
    return None

def push_to_fmc(fmc, name, ace_entries):
    print(f"Pushing Extended ACL with Name: {acl_name} to the FMC")
    push_xacl = fmcapi.ExtendedAccessList(fmc=fmc,
                                            name=name, entries=ace_entries)
    push_xacl.post()

with fmcapi.FMC(
        host=HOST,
        username=USERNAME,
        password=PASSWORD,
        autodeploy=False,
    ) as fmc_obj:

    csv_data_processed = process_csv_data("/Users/vikkushw/Scripts/Secure Firewall/input_data.csv")
    for acl_name, entries in csv_data_processed.items():
        DATA_LIST = []
        for ace_entry in entries:
            built_entry = process_ace(fmc_obj,
                                        acl_name,
                                        ace_entry["ACTION"],
                                        ace_entry["PROTOCOL"],
                                        ace_entry["SOURCETYPE"],
                                        ace_entry["SOURCE"],
                                        ace_entry["SMASK"],
                                        ace_entry["DESTTYPE"],
                                        ace_entry["DESTINATION"],
                                        ace_entry["DMASK"]
                                        )
            if built_entry is None:
                continue
            DATA_LIST.append(built_entry)

        push_to_fmc(fmc_obj, acl_name, DATA_LIST)
        EXTENDED_ACL_NAMES.update({acl_name:len(DATA_LIST)})

print("Configured ACLs:")
for key, value in EXTENDED_ACL_NAMES.items():
    print(f"ACL Name: {key}: Number of Entries: {value}")

print(f"Total Number of ACLs: {len(EXTENDED_ACL_NAMES)}")
