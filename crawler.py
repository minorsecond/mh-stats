#!/bin/python3

import argparse
import datetime
import re
import time

from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import expression
from common import get_info, get_conf, telnet_connect, node_connect, \
    auto_node_selector
from common.string_cleaner import clean_calls
from models.db import local_engine, Node, BadGeocode, CrawledNode

Session = sessionmaker(bind=local_engine)
session = Session()

parser = argparse.ArgumentParser(description="Get node to crawl")
parser.add_argument('--node', metavar='N', type=str, help="Node name to crawl")
parser.add_argument('-v', action='store_true', help='Verbose log')
parser.add_argument('-auto', action='store_true',
                    help="Pick a node to crawl automatically")
args = parser.parse_args()
node_to_crawl = "GMNOD" #args.node
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

def wait_for_response():
    # Send a "?" to trigger the response
    tn.write(b"?\r")

    response = ""
    timeout = 20  # Adjust the initial timeout as needed
    no_text_timeout = 5  # Adjust the duration to wait for no more text
    start_time = time.time()

    while True:
        # Read the immediate response (non-blocking)
        immediate_response = tn.read_very_eager().decode('utf-8')

        # Append the immediate response to the complete response
        response += immediate_response

        # Check if "Nodes" is present in the immediate response
        if "Nodes" in immediate_response:
            break

        # Check if the immediate_response is empty
        if not immediate_response:
            # If there is no text, wait for a duration to ensure it's not a temporary pause
            time.sleep(no_text_timeout)

            # Check again for text after the pause
            immediate_response = tn.read_very_eager().decode('utf-8')

            # If still no text, break the loop
            if not immediate_response:
                break

        # Check if the timeout has been reached
        if time.time() - start_time >= timeout:
            break

    return response

if auto:
    # Get node to crawl from dict
    node_to_crawl = list(node_to_crawl_info.keys())[0]
    print(f"Auto crawling node {node_to_crawl}")

if node_to_crawl:  # Connect to remote
    tn = node_connect(node_to_crawl, tn)
    wait_for_response()
    tn.write("nodes".encode('ascii') + b'\r\n')
    time.sleep(3)
    #tn.write(b'\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n')
    path = node_to_crawl

else:
    path = "KD5LPB-7"
    tn.write("n".encode('ascii') + b"\r")
    tn.write(b'\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n')
    print("3 loop")
print(f"Connected to {conf['telnet_ip']} - {node_to_crawl}")
calls = []

try:
    #output = tn.read_until(b'', timeout=20)
    output = wait_for_response()
    calls = re.findall(r'\w+-\d+', output)
except Exception as e:
    print(f"Error parsing output: {e}")
    exit()

tn.write(b"bye\r")

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

    name_first_part = node_name_pair[0]
    first_base = re.sub(r'[^\w]', ' ', name_first_part.split('-')[0])
    node_name_string = name_first_part

    if len(node_name_pair) == 2:
        name_second_part = node_name_pair[1]
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

                    if len(node_name_pair) == 1:
                        node_match_string = ':' + node_name_pair[0]
                    else:
                        node_match_string = node_name_pair[0] + ':' + node_name_pair[1]

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

                    if base_call not in first_order_nodes and base_call \
                            not in processed_calls:
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

                    elif lon is not None and lat is not None:
                        if verbose:
                            print(f"Updating node {base_call}")
                        session.query(Node). \
                            filter(Node.call == base_call). \
                            update(
                            {Node.geom: f'SRID=4326;POINT({lon} {lat})',
                             Node.last_check: last_check,
                             Node.node_name: node_part},
                            synchronize_session="fetch")
                        updated_counter += 1
                        break
                    else:
                        if verbose:
                            print(f"Couldn't geocode {base_call}")
                        session.query(Node). \
                            filter(Node.call == base_call). \
                            update(
                            {Node.node_name: node_part},
                            synchronize_session="fetch")

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
