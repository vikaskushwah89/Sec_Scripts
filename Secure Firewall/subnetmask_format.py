import ipaddress

def translate_netmask_cidr(ip_address, netmask):
    ip4 = ipaddress.IPv4Network((ip_address,netmask))
    return ip4.with_prefixlen

print(translate_netmask_cidr('10.10.0.0', '255.255.192.0'))