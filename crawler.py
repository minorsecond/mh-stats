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
read_bad_geocodes_cursor.execute('SELECT node_name, last_checked FROM '
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


def remove_dupes(call_list):
    "Returns list with one alias:call pair per node"
    a_list = []
    b_list = []
    res = []

    for element in call_list:
        if len(element) > 1:
            a = element[0]
            b = element[1]

            a_base = re.sub(r'[^\w]', ' ', a.decode('utf-8').split('-')[0])
            b_base = re.sub(r'[^\w]', ' ', b.decode('utf-8').split('-')[0])

            if a_base not in a_list and a_base not in b_list and b_base \
                    not in a_list and b_base not in b_list:
                res.append([a, b])
                a_list.append(a_base)
                b_list.append(b_base)
        else:
            res.append(element)

    return res


def clean_calls(calls):
    """
    Cleans output from telnet, removing : and whitespaces
    """

    cleaned_calls = []
    for call in calls:
        if b':' in call:
            call = call.split(b':')
            while (b'' in call):
                call.remove(b'')
            cleaned_calls.append(call)

        else:
            cleaned_calls.append([None, call])

    cleaned_calls = [[string for string in sublist if string] for sublist in
                     cleaned_calls]
    cleaned_calls = [e for e in cleaned_calls if e != []]
    cleaned_calls = remove_dupes(cleaned_calls)

    return cleaned_calls


processed_node_names = []
no_geocode_counter = 0
added_counter = 0
updated_counter = 0

write_first_order_nodes = con.cursor()
write_bad_geocodes = con.cursor()
clean_call_list = clean_calls(calls)
print(f"{len(first_order_nodes)} exist in DB")

print(f"Processing {len(clean_call_list)} records from BPQ")

for node_name_pair in clean_call_list:
    base_call = None
    point = None
    grid = None
    ssid = None
    name_first_part = node_name_pair[0].decode('utf-8')
    first_base = re.sub(r'[^\w]', ' ', name_first_part.split('-')[0])
    node_name_string = name_first_part

    if len(node_name_pair) == 2:
        name_second_part = node_name_pair[1].decode('utf-8')
        second_base = re.sub(r'[^\w]', ' ', name_second_part.split('-')[0])
        node_name_string += f':{name_second_part}'
    else:
        name_second_part = None
        second_base = None

    if node_name_string not in processed_node_names:
        last_checked = first_order_nodes.get(first_base)
        if not last_checked:  # Try second base
            last_checked = first_order_nodes.get(second_base)
        if not last_checked:  # Might be in bad geocodes table
            last_checked = bad_geocode_calls.get(node_name_string)

        days_lapsed = last_checked and (last_checked - now).days

        # Add new node
        if (days_lapsed and days_lapsed >= refresh_days) or not days_lapsed:
            for check_call in [name_first_part, name_second_part]:
                node_name_part = re.sub(r'[^\w]', ' ',
                                        check_call.split('-')[0])
                print(f"Processing node name part: {node_name_part}")
                info = get_info(node_name_part)
                parent_call = node_name_part
                last_check = now
                order = 1
                path = parent_call

                if info:  # Valid call, but maybe no coords
                    if '-' in check_call:
                        ssid = re.sub(r'[^\w]', ' ', check_call.split('-')[1])
                    else:
                        ssid = None

                    base_call = node_name_part

                    try:
                        lat = float(info[0])
                        lon = float(info[1])
                        grid = info[2]
                        point = Point(lon, lat).wkb_hex
                        added_counter += 1
                        print(f"Got coords for {base_call}")
                    except ValueError:
                        print(f"Error getting coords for {base_call}")
                        point = None

                else:
                    print(f"Couldn't get info for {node_name_part}")

                if base_call not in first_order_nodes:

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

                        print(f"Added {base_call} to node table")

                        # Remove from bad geocode table
                        if base_call in bad_geocode_calls:
                            write_bad_geocodes.executef(
                                f"DELETE FROM packet_mh.bad_geocodes WHERE node_name='{node_name_string}'")
                        break

                else:  # Update node that exists in node table
                    if point:
                        print(f"Updating node {base_call}")
                        update_node_query = f"UPDATE packet_mh.nodes SET geom=st_setsrid('{point}'::geometry, 4326), last_check = now() WHERE call = '{base_call}'"
                        break
                    else:
                        print(f"Couldn't geocode {base_call}")

            # Don't add to bad geocode table if we have coords
            if not point and (
                    node_name_string not in bad_geocode_calls and base_call not in first_order_nodes):
                print(
                    f"Couldn't get coords for {node_name_string}. Adding to bad_geocodes table.")
                write_bad_geocodes.execute(
                    f"INSERT INTO packet_mh.bad_geocodes"
                    f"(last_checked, reason, node_name) "
                    f"VALUES (%s, %s, %s)",
                    (now, 'Bad Add', node_name_string))
                no_geocode_counter += 1

                # Add to dictionary so we don't have
                # multiple entries for each node
                bad_geocode_calls[node_name_part] = now
            elif not point and (node_name_string in bad_geocode_calls):
                # Update attempt time
                print(
                    f"Repeated failure geocoding node {node_name_string}. Updating last checked time.")
                bad_geocode_update_query = f"UPDATE packet_mh.bad_geocodes SET last_checked=now() WHERE node_name = '{node_name_string}';"
                write_bad_geocodes.execute(bad_geocode_update_query)


        elif days_lapsed < refresh_days:
            print(f"Not processing {base_call} as not enough days have passed"
                  f" since last checked")
        processed_node_names.append(base_call)

if processed_node_names == 0:
    print("No nodes added")
else:
    print(f"Processed {len(processed_node_names)} nodes")

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
