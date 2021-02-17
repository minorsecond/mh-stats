#!/bin/python3

import argparse
import re
from string import digits
from telnetlib import Telnet

import psycopg2

from common import get_conf

parser = argparse.ArgumentParser(description="Get node to crawl")
parser.add_argument('--node', metavar='N', type=str, help="Node name to crawl")
args = parser.parse_args()
node_to_crawl = args.node
conf = get_conf()

# Connect to PG
con = psycopg2.connect(database=conf['pg_db'], user=conf['pg_user'],
                       password=conf['pg_pw'], host=conf['pg_host'],
                       port=conf['pg_port'])

remote_mh_cursor = con.cursor()
remote_mh_cursor.execute(
    'SELECT parent_call, remote_call, heard_time, update_time FROM packet_mh.remote_mh')
remote_mh_results = remote_mh_cursor.fetchall()

# Create dictionary to store the values from DB for each parent call
remote_mh_calls = {}
for node in remote_mh_results:
    parent_call = remote_mh_results[0].strip()
    remote_call = remote_mh_results[1].strip()
    heard_time = remote_mh_results[2].strip()
    update_time = remote_mh_results[3].strip()

    if parent_call in remote_mh_calls:
        remote_mh_calls[parent_call].append(
            (remote_call, heard_time, update_time))
    else:  # New dict, create the list of tuples
        remote_mh_calls[parent_call] = [(remote_call, heard_time, update_time)]

# Connect to local telnet server
tn = Telnet(conf['telnet_ip'], conf['telnet_port'], timeout=5)
tn.read_until(b"user: ", timeout=2)
tn.write(conf['telnet_user'].encode('ascii') + b"\r")
tn.read_until(b"password:", timeout=2)
tn.write(conf['telnet_pw'].encode('ascii') + b"\r")
tn.read_until(b'Telnet Server\r\n', timeout=20)
connect_cmd = f"c {node_to_crawl}".encode('ascii')
# tn.write(b"\r\n" + connect_cmd + b"\r")
# tn.read_until(b'Connected to', timeout=20)
# tn.write(b'\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n')
# tn.read_until(b"\n", timeout=20)
tn.write("p".encode('ascii') + b'\r')  # Get available ports
tn.write(b'\n\n\n')
available_ports_raw = tn.read_until(b'\n\n\n',
                                    timeout=20)  # Print available ports to screen

available_ports_raw = available_ports_raw.split(b"Ports")[1].strip()
available_ports_raw = available_ports_raw.split(b'***')[0]
available_ports_raw = re.sub(b' +', b' ', available_ports_raw)
available_ports = available_ports_raw.split(b'\r\n')

# Give menu options on scree
menu_item = 1
print("Select VHF/UHF port to scan MHeard on")
for port in available_ports:
    port = port.decode('utf-8').strip().lstrip(digits)
    print(f"{menu_item}: {port}")
    menu_item += 1

try:
    selected_port = available_ports[int(input()) - 1]
except ValueError:
    print("You didn't enter a valid selection. Closing")
    tn.write(b'bye\r')
    exit()
