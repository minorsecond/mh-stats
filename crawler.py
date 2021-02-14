#!/bin/python3

from telnetlib import Telnet
import re
import psycopg2
import datetime
import configparser
from common import get_info
import time
from shapely.geometry import Point

now = datetime.datetime.now().replace(microsecond=0)
refresh_days = 7

config = configparser.ConfigParser()
config.read("settings.cfg")

telnet_user = config['telnet']['username']
telnet_pw = config['telnet']['password']
telnet_ip = config['telnet']['ip']
telnet_port = config['telnet']['port']
pg_user = config['postgres']['username']
pg_pw = config['postgres']['password']
pg_host = config['postgres']['host']
pg_db = config['postgres']['db']
pg_port = config['postgres']['port']

# Connect to PG
con = psycopg2.connect(database=pg_db, user=pg_user,
                       password=pg_pw, host=pg_host, port=pg_port)

now = datetime.datetime.now().replace(microsecond=0)
year = datetime.date.today().year

read_first_order_node_cursor = con.cursor()
read_first_order_node_cursor.execute('SELECT call, last_check FROM '
                               'packet_mh.nodes WHERE level=1')
first_order_results = read_first_order_node_cursor.fetchall()

first_order_nodes = {}
for node in first_order_results:
    first_order_nodes[node[0].strip()] = node[1]

# Connect to local telnet server
tn = Telnet(telnet_ip, telnet_port, timeout=5)
tn.read_until(b"user: ", timeout=2)
tn.write(telnet_user.encode('ascii') + b"\r")
tn.read_until(b"password:", timeout=2)
tn.write(telnet_pw.encode('ascii') + b"\r")
tn.read_until(b"Connected", timeout=2)
tn.write("n".encode('ascii') + b"\r")
tn.write(b'\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n')
tn.write(b"bye\r")

print(f"Connected to {telnet_ip}")

calls = []
output = tn.read_until(b'***')
output = output.split(b"Nodes")[1].strip()
output = output.split(b'***')[0]
output = re.sub(b' +', b' ', output)
output = output.split(b'\r\n')

for row in output:
    calls.extend(row.split(b' '))


def clean_calls(call_list):
    """
    Cleans output from telnet, removing : and whitespaces
    """

    cleaned_calls = []
    for call in calls:
        if b':' in call:
            call = call.split(b':')[1]
        cleaned_calls.append(call)

    while(b"" in cleaned_calls):
        cleaned_calls.remove(b"")

    return cleaned_calls


processed_calls = []
bad_geocode_calls = []
added_counter = 0
updated_counter = 0

write_first_order_nodes = con.cursor()
write_bad_geocodes = con.cursor()
for call in clean_calls(calls):
    point = None
    call = call.decode('utf-8')
    base_call = re.sub(r'[^\w]', ' ', call.split('-')[0])
    if base_call not in processed_calls:
        if '-' in call:
            ssid = re.sub(r'[^\w]', ' ', call.split('-')[1])
        else:
            ssid = None

        last_checked = first_order_nodes.get(base_call)

        # Add new node
        if base_call not in first_order_nodes:
            print(f"Attempting to add node {base_call}")
            info = get_info(base_call)
            parent_call = base_call
            last_check = now
            order = 1
            path = parent_call

            if info:
                try:
                    lat = float(info[0])
                    lon = float(info[1])
                    grid = info[2]
                    point = Point(lon, lat).wkb_hex
                    added_counter += 1
                except ValueError:
                    print(f"Error getting coordinates for {base_call}")
                    point = None

            if point:
                write_first_order_nodes.execute(f"INSERT INTO packet_mh.nodes "
                                                f"(call, parent_call, last_check, "
                                                f"geom, ssid, path, level, grid) VALUES "
                                                f"(%s, %s, %s, "
                                                f"st_setsrid('{point}'::geometry, 4326), "
                                                f"%s, %s, %s, %s)", (
                                                base_call, parent_call, last_check,
                                                ssid, path, order, grid))
            else:
                print(f"Couldn't get coords for {base_call}. Adding to bad_geocodes table.")
                if base_call not in bad_geocode_calls:
                    write_bad_geocodes.execute(f"INSERT INTO packet_mh.bad_geocodes"
                                               f"(call, last_checked, reason) "
                                               f"VALUES (%s, %s, %s)",
                                               (base_call, now, 'Bad Add'))
                    bad_geocode_calls.append(base_call)
        elif last_checked and (now - last_checked).days >= 14 or not last_checked:
            print(f"Updating node {base_call}")
            info = get_info(base_call)
            parent_call = base_call
            last_check = now
            order = 1
            path = parent_call

            if info:
                try:
                    lat = float(info[0])
                    lon = float(info[1])
                    point = Point(lon, lat).wkb_hex
                    updated_counter += 1
                except ValueError:
                    print(f"Error getting coordinates for {base_call}")
                    point = None

            if point:
                update_fon = f"UPDATE packet_mh.nodes SET geom = " \
                             f"st_setsrid('{point}'::geometry, 4326), " \
                             f"last_check=now() WHERE call = '{base_call}';"
                write_first_order_nodes.execute(update_fon)

            else:
                if base_call not in bad_geocode_calls:
                    write_bad_geocodes.execute(
                        f"INSERT INTO packet_mh.bad_geocodes"
                        f"(call, last_checked, reason) VALUES (%s, %s, %s)",
                        (base_call, now, 'Bad Update'))
                    bad_geocode_calls.append(base_call)
        processed_calls.append(base_call)

if processed_calls == 0:
    print("No nodes added")
else:
    print(f"Added {len(processed_calls)} nodes")

if updated_counter == 0:
    print("No nodes updated")
else:
    print(f"Updated {updated_counter} nodes")

if len(bad_geocode_calls) == 0:
    print("No errors encountered")
else:
    print(f"{len(bad_geocode_calls)} errors encountered")

con.commit()
con.close()
