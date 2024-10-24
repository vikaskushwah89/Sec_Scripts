import csv
import fmcapi
import ipaddress

HOST = "10.127.245.194"
USERNAME = "vicky"
PASSWORD = "V!cky!123"
EXTENDED_ACL_NAMES = []
DATA_LIST = []
ACE_NAME = ""

def translate_netmask_cidr(ip_address, netmask):
    ip4 = ipaddress.IPv4Network((ip_address,netmask))
    return ip4.with_prefixlen

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

    else:
        print(f"Non IP based ACL entry found for ACL {name}. Skipping Line")
        return None

with fmcapi.FMC(
        host=HOST,
        username=USERNAME,
        password=PASSWORD,
        autodeploy=False,
    ) as fmc_obj:

    with open('/Users/vikkushw/Scripts/Secure Firewall/input_data.csv', mode ='r') as file:
        csvFile = csv.DictReader(file)
        for acl_row in csvFile:
            if acl_row["NAME"] in EXTENDED_ACL_NAMES:
                ace_entry = process_ace(fmc_obj,
                                        acl_row["NAME"],
                                        acl_row["ACTION"],
                                        acl_row["PROTOCOL"],
                                        acl_row["SOURCETYPE"],
                                        acl_row["SOURCE"],
                                        acl_row["SMASK"],
                                        acl_row["DESTTYPE"],
                                        acl_row["DESTINATION"],
                                        acl_row["DMASK"]
                                        )
                if ace_entry is None:
                    continue
                DATA_LIST.append(ace_entry)
            else:
                if EXTENDED_ACL_NAMES != []:
                    print(f"Pushing Extended ACL with Name: {ACE_NAME} to the FMC")
                    push_xacl = fmcapi.ExtendedAccessList(fmc=fmc_obj,
                                                        name=ACE_NAME, entries=DATA_LIST)
                    push_xacl.post()
                    DATA_LIST = []
                EXTENDED_ACL_NAMES.append(acl_row["NAME"])
                print(f"Creating Extended ACL with Name: {acl_row['NAME']}")
                ace_entry = process_ace(fmc_obj,
                                        acl_row["NAME"],
                                        acl_row["ACTION"],
                                        acl_row["PROTOCOL"],
                                        acl_row["SOURCETYPE"],
                                        acl_row["SOURCE"],
                                        acl_row["SMASK"],
                                        acl_row["DESTTYPE"],
                                        acl_row["DESTINATION"],
                                        acl_row["DMASK"]
                                        )
                if ace_entry is None:
                    continue
                DATA_LIST.append(ace_entry)
                ACE_NAME = acl_row["NAME"]

        print(f"Pushing Extended ACL with Name: {ACE_NAME} to the FMC")
        push_xacl = fmcapi.ExtendedAccessList(fmc=fmc_obj,
                                            name=ACE_NAME, entries=DATA_LIST)
        push_xacl.post()
        DATA_LIST = []

print(f"Configured ACLs:\n{EXTENDED_ACL_NAMES}\nTotal Number of ACLs: {len(EXTENDED_ACL_NAMES)}")