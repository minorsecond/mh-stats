#!/bin/python3

import datetime
import re
from telnetlib import Telnet

import psycopg2
from shapely.geometry import Point

from common import get_info, get_conf

conf = get_conf()

now = datetime.datetime.now().replace(microsecond=0)
refresh_days = 7

# Connect to PG
con = psycopg2.connect(database=conf['pg_db'], user=conf['pg_user'],
                       password=conf['pg_pw'], host=conf['pg_host'],
                       port=conf['pg_port'])

year = datetime.date.today().year

read_first_order_node_cursor = con.cursor()
read_first_order_node_cursor.execute('SELECT call, last_check FROM '
                                     'packet_mh.nodes WHERE level=1')
first_order_results = read_first_order_node_cursor.fetchall()

read_bad_geocodes_cursor = con.cursor()
read_bad_geocodes_cursor.execute('SELECT call, last_checked FROM '
                                 'packet_mh.bad_geocodes')
bad_geocode_results = read_bad_geocodes_cursor.fetchall()

first_order_nodes = {}
for node in first_order_results:
    first_order_nodes[node[0].strip()] = node[1]

bad_geocode_calls = {}
for bad_call in bad_geocode_results:
    bad_geocode_calls[(bad_call[0])] = bad_call[1]

# Connect to local telnet server
tn = Telnet(conf['telnet_ip'], conf['telnet_port'], timeout=5)
tn.read_until(b"user: ", timeout=2)
tn.write(conf['telnet_user'].encode('ascii') + b"\r")
tn.read_until(b"password:", timeout=2)
tn.write(conf['telnet_pw'].encode('ascii') + b"\r")
tn.read_until(b"Connected", timeout=2)
tn.write("n".encode('ascii') + b"\r")
tn.write(b'\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n')
tn.write(b"bye\r")

print(f"Connected to {conf['telnet_ip']}")

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
no_geocode_counter = 0
added_counter = 0
updated_counter = 0

write_first_order_nodes = con.cursor()
write_bad_geocodes = con.cursor()
clean_call_list = clean_calls(calls)
print(f"{len(first_order_nodes)} exist in DB")

data_size = len(set([re.sub(r'[^\w]', ' ', call.decode('utf-8').
                            split('-')[0]) for call in clean_call_list]))

print(f"Processing {data_size} records to add")

for call in clean_call_list:
    point = None
    grid = None
    call = call.decode('utf-8')
    base_call = re.sub(r'[^\w]', ' ', call.split('-')[0])
    if base_call not in processed_calls:
        last_checked = first_order_nodes.get(base_call)

        if not last_checked:  # Might be in bad geocodes table
            last_checked = bad_geocode_calls.get(base_call)

        days_lapsed = last_checked and (last_checked - now).days
        if '-' in call:
            ssid = re.sub(r'[^\w]', ' ', call.split('-')[1])
        else:
            ssid = None

        # Add new node
        if (days_lapsed and days_lapsed >= refresh_days) or not days_lapsed:
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

            if base_call not in first_order_nodes:
                print(f"Attempting to add node {base_call}")

                if point:
                    write_first_order_nodes.execute(
                        f"INSERT INTO packet_mh.nodes "
                        f"(call, parent_call, last_check, "
                        f"geom, ssid, path, level, grid) VALUES "
                        f"(%s, %s, %s, "
                        f"st_setsrid('{point}'::geometry, 4326), "
                        f"%s, %s, %s, %s)", (
                            base_call, parent_call, last_check,
                            ssid, path, order, grid))

                    # Remove from bad geocode table
                    if base_call in bad_geocode_calls:
                        write_bad_geocodes.executef(
                            f"DELETE FROM packet_mh.bad_geocodes WHERE call='{base_call}'")

                else:  # Don't add to bad geocode table if we have coords
                    if base_call not in bad_geocode_calls or first_order_nodes:
                        print(
                            f"Couldn't get coords for {base_call}. Adding to bad_geocodes table.")
                        write_bad_geocodes.execute(
                            f"INSERT INTO packet_mh.bad_geocodes"
                            f"(call, last_checked, reason) "
                            f"VALUES (%s, %s, %s)",
                            (base_call, now, 'Bad Add'))
                        no_geocode_counter += 1
                    else:
                        # Update attempt time
                        print(
                            f"Repeated failure geocoding node {base_call}. Updating last checked time.")
                        bad_geocode_update_query = f"UPDATE packet_mh.bad_geocodes SET last_checked=now() WHERE call = '{base_call}';"
                        write_bad_geocodes.execute(bad_geocode_update_query)

            else:  # Update node that exists in node table
                if point:
                    print(f"Updating node {base_call}")
                    update_node_query = f"UPDATE packet_mh.nodes SET geom=st_setsrid('{point}'::geometry, 4326), last_check = now() WHERE call = '{base_call}'"
                else:
                    print(f"Couldn't geocode {base_call}")
        elif days_lapsed < refresh_days:
            print(f"Not processing {base_call} as not enough days have passed"
                  f"since last checked")
        processed_calls.append(base_call)

if processed_calls == 0:
    print("No nodes added")
else:
    print(f"Added {len(processed_calls)} nodes")

if updated_counter == 0:
    print("No nodes updated")
else:
    print(f"Updated {updated_counter} nodes")

if no_geocode_counter == 0:
    print("No errors encountered")
else:
    print(f"{no_geocode_counter} errors encountered")

con.commit()
con.close()
