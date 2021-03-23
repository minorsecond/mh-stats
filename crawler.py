#!/bin/python3

import argparse
import datetime
import re

from sqlalchemy.orm import sessionmaker

from common import get_info, get_conf, telnet_connect, node_connect, \
    auto_node_selector
from models.db import local_engine, Node, BadGeocode, CrawledNode

Session = sessionmaker(bind=local_engine)
session = Session()

parser = argparse.ArgumentParser(description="Get node to crawl")
parser.add_argument('--node', metavar='N', type=str, help="Node name to crawl")
parser.add_argument('-v', action='store_true', help='Verbose log')
parser.add_argument('-auto', action='store_true',
                    help="Pick a node to crawl automatically")
args = parser.parse_args()
node_to_crawl = args.node
verbose = args.v
auto = args.auto
conf = get_conf()
info_method = conf['info_method']
refresh_days = 7

node_to_crawl_info = None
if auto and node_to_crawl:
    print("You can't enter node to crawl & auto mode")
    exit()

elif auto:
    node_to_crawl_info = auto_node_selector(CrawledNode, session, refresh_days)

now = datetime.datetime.utcnow().replace(microsecond=0)
print(f"\n==============================================\n"
      f"Run at {now}")

# Connect to PG

year = datetime.date.today().year

first_order_results = session.query(Node.call, Node.last_check).filter(
    Node.level == 1).all()
bad_geocode_results = session.query(BadGeocode.node_name,
                                    BadGeocode.last_checked).all()

first_order_nodes = {}
for node in first_order_results:
    node_call = node.call.strip()
    node_last_check = node.last_check
    first_order_nodes[node_call] = node_last_check

bad_geocode_calls = {}
for bad_call in bad_geocode_results:
    bad_geocode_node_name = bad_call.node_name.strip()
    bad_geocode_last_checked = bad_call.last_checked
    bad_geocode_calls[bad_geocode_node_name] = bad_geocode_last_checked

# Connect to local telnet server
tn = telnet_connect()

if auto:
    # Get node to crawl from dict
    node_to_crawl = list(node_to_crawl_info.keys())[0]
    print(f"Auto crawling node {node_to_crawl}")

if node_to_crawl:  # Connect to remote
    tn = node_connect(node_to_crawl, tn)
    tn.write("n".encode('ascii') + b'\r')
    tn.write(b'\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n')
    path = node_to_crawl

else:
    path = "KD5LPB-7"
    tn.write("n".encode('ascii') + b"\r")
    tn.write(b'\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n')
print(f"Connected to {conf['telnet_ip']} - {node_to_crawl}")
tn.write(b"bye\r")
calls = []

try:
    output = tn.read_until(b'\n', timeout=20)
    input(output)
    output = output.split(b"Nodes")[1].strip()
    output = output.split(b'***')[0]
    output = re.sub(b' +', b' ', output)
    output = output.split(b'\r\n')
except Exception as e:
    print(f"Error parsing output: {e}")
    exit()

for row in output:
    calls.extend(row.split(b' '))

# Get nodes with traffic to determine BPQ nodes
nt = None
try:
    tn.write(b'\n\n\n')
    tn.read_until(b'\n')
    tn.write(b'n t\r')
    tn.write(b'\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n')
    nt = tn.read_until(b'\n')
    print(nt)
    nt = nt.split(b"Nodes")[1].strip()
    nt = nt.split(b"***")[0]
    nt = re.sub(b' +', b' ', nt)
    nt = nt.split(b'\r\n')
except Exception as e:
    print(f"Error getting n t info: {e}")
    nt = None
input(nt)

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


def clean_calls(calls_to_clean):
    """
    Cleans output from telnet, removing : and whitespaces
    """

    cleaned_calls = []
    for call in calls_to_clean:
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
processed_calls = []
no_geocode_counter = 0
added_counter = 0
updated_counter = 0

clean_call_list = clean_calls(calls)
print(f"{len(first_order_nodes)} exist in DB")

print(f"Processing {len(clean_call_list)} records from BPQ")
new_nodes = 0
for node_name_pair in clean_call_list:
    base_call = None
    lon = None
    lat = None
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
        if not last_checked:
            for call_node_pair in bad_geocode_calls:
                if name_first_part in call_node_pair:
                    last_checked = bad_geocode_calls.get(call_node_pair)
                elif name_second_part in bad_geocode_calls:
                    last_checked = bad_geocode_calls.get(call_node_pair)
        try:
            days_lapsed = (now - last_checked).days
        except TypeError:
            days_lapsed = None

        # Add new node
        if days_lapsed is None or (days_lapsed >= refresh_days):
            part = 0
            for check_call in [name_first_part, name_second_part]:
                if check_call:
                    if '-' in check_call:
                        call_part = re.sub(r'[^\w]', ' ',
                                           check_call.split('-')[0])
                    else:
                        call_part = check_call
                    if verbose:
                        print(f"Processing node name part: {call_part}")
                    info = get_info(call_part, info_method)
                    parent_call = call_part.upper()
                    last_check = now
                    order = 1

                    node_part = None
                    if part == 0:
                        node_part = name_second_part
                    elif part == 1:
                        node_part = name_first_part

                    if info:  # Valid call, but maybe no coords
                        if '-' in check_call:
                            ssid = re.sub(r'[^\w]', ' ',
                                          check_call.split('-')[1])
                            ssid = int(ssid)
                        else:
                            ssid = None

                        base_call = call_part.upper()

                        try:
                            lat = float(info[0])
                            lon = float(info[1])
                            grid = info[2]
                            added_counter += 1
                            if verbose:
                                print(f"Got coords for {base_call}")
                        except ValueError:
                            if verbose:
                                print(f"Error getting coords for {base_call}")

                        if node_part is None:
                            node_part = base_call

                    # Update timestamp if call has been geocoded but now can't
                    # get coords
                    elif not info and last_checked is not None:
                        session.query(Node). \
                            filter(Node.call == call_part). \
                            update({Node.last_check: last_check,
                                    Node.node_name: node_part},
                                   synchronize_session="fetch")
                    elif verbose:
                        print(f"Couldn't get info for {call_part}")

                    if base_call not in first_order_nodes and base_call not in processed_calls:
                        if lon is not None and lat is not None:

                            new_node = Node(
                                call=base_call,
                                parent_call=parent_call,
                                last_check=last_check,
                                geom=f'SRID=4326;POINT({lon} {lat})',
                                ssid=ssid,
                                path=path,
                                level=order,
                                grid=grid,
                                node_name=node_part
                            )

                            session.add(new_node)

                            print(f"Added {base_call} to node table")
                            new_nodes += 1
                            # Remove from bad geocode table
                            if base_call in bad_geocode_calls:
                                session.query(BadGeocode).filter(
                                    BadGeocode.node_name == node_name_string).delete()
                            break

                        processed_calls.append(base_call)

                    else:  # Update node that exists in node table
                        if lon is not None and lat is not None:
                            if verbose:
                                print(f"Updating node {base_call}")
                            session.query(Node). \
                                filter(Node.call == base_call). \
                                update(
                                {Node.geom: f'SRID=4326;POINT({lon} {lat})',
                                 Node.last_check: last_check,
                                 Node.node_name: node_part},
                                synchronize_session="fetch")
                            break
                        else:
                            if verbose:
                                print(f"Couldn't geocode {base_call}")

                        processed_calls.append(base_call)
                part += 1

            # Don't add to bad geocode table if we have coords
            if (lat is None or lon is None) and (
                    node_name_string not in bad_geocode_calls and base_call not in first_order_nodes):
                if verbose:
                    print(
                        f"Couldn't get coords for {node_name_string}. Adding to bad_geocodes table.")
                new_bad_geocode = BadGeocode(
                    last_checked=now,
                    reason="Bad Add",
                    node_name=node_name_string
                )
                session.add(new_bad_geocode)
                no_geocode_counter += 1

                # Add to dictionary so we don't have
                # multiple entries for each node
                bad_geocode_calls[call_part] = now
            elif (lat is None or lon is None) and (
                    node_name_string in bad_geocode_calls):
                # Update attempt time
                if verbose:
                    print(
                        f"Repeated failure geocoding node {node_name_string}. Updating last checked time.")
                session.query(BadGeocode).filter(
                    BadGeocode.node_name == node_name_string).update(
                    {BadGeocode.last_checked: now},
                    synchronize_session="fetch")

        elif days_lapsed < refresh_days:
            if verbose:
                print(
                    f"Not processing {node_name_string} as not enough days have passed"
                    f" since last checked")
        processed_node_names.append(node_name_string)

if new_nodes == 0:
    print("No nodes added")
else:
    print(f"Processed {new_nodes} nodes")

if updated_counter == 0:
    print("No nodes updated")
else:
    print(f"Updated {updated_counter} nodes")

if no_geocode_counter == 0:
    print("No errors encountered")
else:
    print(f"{no_geocode_counter} errors encountered")

session.commit()
session.close()
