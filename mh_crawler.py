#!/bin/python3

import argparse
import datetime
import re
from string import digits
from telnetlib import Telnet

from geoalchemy2.shape import to_shape
from sqlalchemy import func, or_
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.exc import NoResultFound

from common import get_info, get_conf
from models.db import engine, CrawledNode, RemoteOperator, RemoteDigipeater, \
    RemotelyHeardStation, BadGeocode

refresh_days = 1
debug = False

port_name = None

parser = argparse.ArgumentParser(description="Crawl BPQ nodes")
parser.add_argument('--node', metavar='N', type=str, help="Node name to crawl")
parser.add_argument('-auto', action='store_true',
                    help="Pick a node to crawl automatically")
args = parser.parse_args()
node_to_crawl = args.node
auto = args.auto
conf = get_conf()

if node_to_crawl:
    node_to_crawl = node_to_crawl.strip().upper()

now = datetime.datetime.utcnow().replace(microsecond=0)
year = datetime.date.today().year

# Connect to PG
Session = sessionmaker(bind=engine)

last_crawled_port_name = None
node_to_crawl_info = {}
read_crawled_nodes = Session()
session = Session()

if auto and node_to_crawl:
    print("You can't enter node to crawl & auto mode")
    exit()

elif auto and not debug:
    refresh_time = now - datetime.timedelta(days=refresh_days)
    # Get a node that hasn't been crawled in 2 weeks

    try:
        # Get a node port that doesn't need check and is active
        crawled_nodes = session.query(CrawledNode).filter(
            CrawledNode.last_crawled < refresh_time). \
            filter(CrawledNode.needs_check == False,
                   CrawledNode.active_port == True). \
            order_by(func.random()).limit(1).one_or_none()
        if crawled_nodes:
            node_to_crawl_info = {
                crawled_nodes.node_id: (
                    crawled_nodes.id,
                    crawled_nodes.port,
                    crawled_nodes.last_crawled,
                    crawled_nodes.port_name
                )
            }
    except NoResultFound:
        print("Nothing to crawl")
        exit()

elif not node_to_crawl and not debug:
    print("You must enter a node to crawl.")
    exit()
elif not node_to_crawl and not auto:
    node_to_crawl = "KD5LPB"

# Get bad geocode node names
existing_bad_geocodes = session.query(BadGeocode.node_name).all()
bad_geocodes = []
for geocode in existing_bad_geocodes:
    geocode = geocode[0]

    if ':' in geocode:
        node_part_one = geocode.split(':')[0].split('-')[0]
        node_part_two = geocode.split(':')[1].split('-')[0]
        bad_geocodes.append(node_part_one)
        bad_geocodes.append(node_part_two)
    else:
        bad_geocodes.append(geocode)

# Get all remote operators
existing_ops = session.query(RemoteOperator).all()

existing_ops_data = {}
for op in existing_ops:
    remote_call = op.remote_call
    point = to_shape(op.geom)
    lon = point.x
    lat = point.y

    existing_ops_data[remote_call] = (lat, lon)

# Get all remote digipeaters
existing_digipeaters = session.query(RemoteDigipeater).all()

existing_digipeaters_data = {}
for digipeater in existing_digipeaters:
    digipeater_call = digipeater.call
    point = to_shape(digipeater.geom)
    lon = point.x
    lat = point.y
    heard = digipeater.heard
    existing_digipeaters_data[digipeater_call] = (lat, lon, heard)

# Get all remote MHeard list
existing_remote_mh_results = session.query(RemotelyHeardStation).all()

existing_mh_data = []
for row in existing_remote_mh_results:
    call = row.remote_call
    timestamp = row.heard_time
    hms = timestamp.strftime("%H:%M:%S")
    existing_mh_data.append(f"{call} {hms}")

# Connect to local telnet server
tn = Telnet(conf['telnet_ip'], conf['telnet_port'], timeout=5)
tn.read_until(b"user: ", timeout=2)
tn.write(conf['telnet_user'].encode('ascii') + b"\r")
tn.read_until(b"password:", timeout=2)
tn.write(conf['telnet_pw'].encode('ascii') + b"\r")
tn.read_until(b'Telnet Server\r\n', timeout=20)

if auto and not debug:
    # Get node to crawl from dict
    node_to_crawl = list(node_to_crawl_info.keys())[0]
    print(f"Auto crawling node {node_to_crawl}")

connect_cmd = f"c {node_to_crawl}".encode('ascii')
if not debug:  # Stay local if debugging
    try:
        print(f"Connecting to {node_to_crawl}")
        tn.write(b"\r\n" + connect_cmd + b"\r")
        con_results = tn.read_until(b'Connected to', timeout=30)

        # Stuck on local node
        if con_results == b'\r\n' or \
                b"Downlink connect needs port number" in con_results:
            print(f"Couldn't connect to {node_to_crawl}")
            tn.write(b'b\r')
            exit()
        else:
            print(f"Connected to {node_to_crawl}")
        tn.write(b'\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n')
        tn.read_until(b"\n", timeout=20)
    except KeyboardInterrupt:
        print("Closing connection")
        tn.write(b'bye\r')
        exit()

# Get available ports
tn.write("p".encode('ascii') + b'\r')  # Get available ports
tn.write(b'\n\n\n')
available_ports_raw = tn.read_until(b'\n\n\n',
                                    timeout=20)  # Print available ports to screen

try:
    available_ports_raw = available_ports_raw.split(b"Ports")[1].strip()
    available_ports_raw = available_ports_raw.split(b'***')[0]
    available_ports_raw = re.sub(b' +', b' ', available_ports_raw)
    available_ports = available_ports_raw.split(b'\r\n')
except IndexError:
    # Corrupt data
    print(f"Possible corrupt data received: {available_ports_raw}")
    exit()

if not auto:

    # Give menu options on screen
    selected_port = None
    menu_item = 1
    print("Select VHF/UHF port to scan MHeard on")
    for port in available_ports:
        port = port.decode('utf-8').strip().lstrip(digits)
        print(f"{menu_item}: {port}")
        menu_item += 1

    try:
        selected_port = int(input().strip())
    except ValueError:
        print("You didn't enter a valid selection. Closing")
        tn.write(b'bye\r')
        exit()

else:
    # crawled_node[1]: (crawled_node[0], crawled_node[2], crawled_node[3])
    selected_port = node_to_crawl_info.get(node_to_crawl)[1]

if selected_port:
    port_name = available_ports[selected_port - 1].decode(
        'utf-8').strip().lstrip(digits).strip()

    # Add the UID if it doesn't exist
    crawled_node_uid = node_to_crawl + '-' + port_name
    session.query(CrawledNode).filter(CrawledNode.node_id == node_to_crawl,
                                      CrawledNode.port == selected_port,
                                      or_(CrawledNode.port_name == port_name,
                                          CrawledNode.port_name.is_(None)),
                                      CrawledNode.uid.is_(None)). \
        update({CrawledNode.uid: crawled_node_uid},
               synchronize_session="fetch")

    # Update needs_check flag and exit if port has changed
    if last_crawled_port_name and port_name.strip() != last_crawled_port_name.strip():
        print(f"Port has changed for {node_to_crawl}")
        session.query(CrawledNode).filter(CrawledNode.node_id ==
                                          f'{node_to_crawl}'). \
            update({CrawledNode.needs_check: True},
                   synchronize_session='fetch')
        exit()

    # Send the MH command
    print(f"Getting MH list for port {selected_port}.")
    mh_command = f"mh {selected_port}".encode('ascii')
    tn.write(mh_command + b"\r")
    tn.write(b"\r")
    tn.write(b"bye\r")
else:
    print("No port selected")
    exit()

mh_output = None
try:
    # Read and parse output from telnet
    mh_output = tn.read_until(b'***', timeout=20)
    port_string = f"Port {selected_port}".encode('ascii')
    mh_output = mh_output.split(port_string)[1].strip()
    mh_output = mh_output.split(b'***')[0]
    mh_output = re.sub(b' +', b' ', mh_output)
    mh_output = mh_output.split(b'\r\n')
    print("Got MH list")
except IndexError:
    print(f"Might have gotten bad results. The MH output was {mh_output}")
    print("Try again")
    exit()

# Make list of lists for each entry
index = 0
for item in mh_output:
    mh_output[index] = item.strip().split(b' ')
    index += 1

# Clean up list
for index, item in enumerate(mh_output):
    if b'via' in item:
        item.remove(b'via')

    if len(item) == 1:
        del mh_output[index]

# Convert time to datetime, and get digipeater list
mh_list = []
for item in mh_output:
    if len(item) > 1:
        res = []
        call = item[0].decode('utf-8')
        res.append(call)
        time_passed = item[1].decode('utf-8')
        days_passed = int(time_passed.split(':')[0])
        hours_passed = int(time_passed.split(':')[1])
        minutes_passed = int(time_passed.split(':')[2])
        seconds_passed = int(time_passed.split(':')[3])

        try:
            ymd = now - datetime.timedelta(days=days_passed,
                                           hours=hours_passed,
                                           minutes=minutes_passed,
                                           seconds=seconds_passed)
            res.append(ymd)
        except Exception as e:
            # Got bad time format
            print(f"Error parsing time passed: {time_passed}. Error: {e}")
            exit()
        try:
            digipeaters = item[4].decode('utf-8').split(',')
            res.append(digipeaters)
        except IndexError:
            res.append(None)

        mh_list.append(res)

mh_list = sorted(mh_list, key=lambda x: x[1], reverse=False)

# Update the nodes crawled table
node_info = node_to_crawl_info.get(node_to_crawl)

if auto and not debug:
    # Update timestamp of crawled node
    # crawled_node[1]: (crawled_node[0], crawled_node[2], crawled_node[3])

    print("Updating crawled node timestamp")
    nodes_to_crawl_id = node_info[0]
    port = node_info[1]
    timestamp = node_info[2]

    session.query(CrawledNode).filter(
        CrawledNode.id == nodes_to_crawl_id).update(
        {CrawledNode.last_crawled: now}, synchronize_session="fetch")

    session.query(CrawledNode).filter(CrawledNode.id == nodes_to_crawl_id,
                                      CrawledNode.needs_check.is_(None)). \
        update({CrawledNode.needs_check: False}, synchronize_session="fetch")

    if not last_crawled_port_name:  # Update port name if doesn't exist
        session.query(CrawledNode).filter(
            CrawledNode.id == nodes_to_crawl_id).update(
            {CrawledNode.port_name: port_name}, synchronize_session="fetch")

elif not debug:  # Write new node
    crawled_nodes = session.query(CrawledNode).filter(
        CrawledNode.node_id == node_to_crawl,
        CrawledNode.port == selected_port).one_or_none()

    if crawled_nodes:
        nodes_to_crawl_id = crawled_nodes.id

        # Populate needs check field if null
        session.query(CrawledNode).filter(CrawledNode.id == nodes_to_crawl_id,
                                          CrawledNode.needs_check.is_(None)). \
            update({CrawledNode.needs_check: False,
                    CrawledNode.active_port: True},
                   synchronize_session="fetch")

        if selected_port and node_to_crawl and selected_port and last_crawled_port_name is None:
            print("Adding port name to existing row")
            session.query(CrawledNode).filter(
                CrawledNode.id == nodes_to_crawl_id).update(
                {CrawledNode.port_name: port_name,
                 CrawledNode.last_crawled: now}, synchronize_session="fetch")

    # Add new item to table
    elif not crawled_nodes and selected_port and node_to_crawl:
        print(f"Adding {node_to_crawl} to crawled nodes table")
        new_crawled_node = CrawledNode(
            node_id=node_to_crawl,
            port=selected_port,
            last_crawled=now,
            port_name=port_name,
            needs_check=False,
            uid=f"{node_to_crawl}-{port_name}",
            active_port=True
        )
        session.add(new_crawled_node)

    else:
        print(f"Something bad happened. Crawled node results: {crawled_nodes}")

# Do the MH List Processing
digipeater_list = {}
current_op_list = []

for item in mh_list:
    timedelta = None
    info = None

    # Call includes ssid, ie KD5LPB-7
    call = item[0].strip().upper()
    # Op_call is just the call, ie KD5LPB
    op_call = re.sub(r'[^\w]', ' ', call.split('-')[0].strip())

    # Get SSID of call if it exists
    if '-' in call:
        ssid = int(re.sub(r'[^\w]', ' ', call.split('-')[1]))
    else:
        ssid = None

    timestamp = item[1]

    # Get last time station was heard
    try:
        last_heard = session.query(RemoteOperator.lastheard). \
            distinct(RemoteOperator.remote_call, RemoteOperator.lastheard). \
            filter(RemoteOperator.remote_call == f'{op_call}'). \
            order_by(RemoteOperator.lastheard).first()

        if last_heard:
            last_heard = last_heard[0]

            timedelta = (now - last_heard)
    except IndexError:
        timedelta = None
        last_heard = None

    hms = timestamp.strftime("%H:%M:%S")
    lat = None
    lon = None
    grid = None

    digipeaters = ""
    try:
        for digipeater in item[2]:
            digipeater = digipeater.strip()
            digipeaters += f"{digipeater},"
            digipeater_list[digipeater] = timestamp
    except TypeError:
        digipeaters = None

    # Write MH table
    if f"{call} {hms}" not in existing_mh_data:
        print(f"{now} Adding {call} at {timestamp} through {digipeaters}.")

        remotely_heard = RemotelyHeardStation(
            parent_call=node_to_crawl,
            remote_call=call,
            heard_time=timestamp,
            ssid=ssid,
            update_time=now,
            port=port_name,
            uid=f"{node_to_crawl}-{port_name}"
        )

        session.add(remotely_heard)

    # Update ops last heard
    if last_heard and timestamp > last_heard:
        session.query(RemoteOperator). \
            filter(RemoteOperator.remote_call == f"{op_call}"). \
            update({RemoteOperator.lastheard: timestamp},
                   synchronize_session="fetch")

    # Write Ops table if new operator
    if op_call not in existing_ops_data and op_call not in current_op_list:
        # add coordinates & grid
        if '-' in call:
            try:
                info = get_info(call.split('-')[0])
            except Exception as e:
                print(f"Error {e} on {call}")

        else:
            try:
                info = get_info(call)
            except Exception as e:
                print(f"Error {e} on {call}")

        if info:
            try:
                lat = float(info[0])
                lon = float(info[1])
                grid = info[2]
            except ValueError:
                print(f"Couldn't get coordinates for {op_call}")
                grid = None

        if grid:  # No grid means no geocode generally
            print(f"{now} Adding {op_call} to operator table.")

            remote_operator = RemoteOperator(
                parent_call=node_to_crawl,
                remote_call=op_call,
                lastheard=timestamp,
                grid=grid,
                geom=f'SRID=4326;POINT({lon} {lat})',
                port=port_name,
                uid=f"{node_to_crawl}-{port_name}"
            )

            session.add(remote_operator)

        else:  # Add to bad_geocodes table
            if op_call not in bad_geocodes:
                print(f"{op_call} not geocoded. Adding to bad geocode table")
                new_bad_geocode = BadGeocode(
                    last_checked=now,
                    reason="Operator not geocoded",
                    node_name=op_call,
                    parent_node=node_to_crawl
                )

                session.add(new_bad_geocode)

    elif op_call not in current_op_list:  # Update existing op
        if timedelta and timedelta.days >= refresh_days:
            # add coordinates & grid

            info = get_info(call.split('-')[0])

            if info:
                try:
                    lat = float(info[0])
                    lon = float(info[1])
                    grid = info[2]
                except IndexError:
                    lat = None
                    lon = None
                    grid = None

                print(f"Updating coordinates for {op_call}")
                if lat is not None and lon is not None:
                    session.query(RemoteOperator).filter(
                        RemoteOperator.remote_call == f'{op_call}').update(
                        {RemoteOperator.parent_call: node_to_crawl,
                         RemoteOperator.geom: f"SRID=4326;POINT({lon} {lat})",
                         RemoteOperator.lastheard: timestamp,
                         RemoteOperator.grid: grid,
                         RemoteOperator.port: port_name,
                         RemoteOperator.uid: f"{node_to_crawl}-{port_name}"},
                        synchronize_session="fetch")

        else:  # Update port & uid
            print(f"Updating entry for {op_call}")
            session.query(RemoteOperator).filter(
                RemoteOperator.remote_call == f'{op_call}').update(
                {RemoteOperator.parent_call: node_to_crawl,
                 RemoteOperator.lastheard: timestamp,
                 RemoteOperator.port: port_name,
                 RemoteOperator.uid: f"{node_to_crawl}-{port_name}"},
                synchronize_session="fetch")

    current_op_list.append(op_call)

# Write digipeaters table
added_digipeaters = []
for digipeater in digipeater_list.items():
    lat = None
    lon = None
    grid = None
    digipeater_call = digipeater[0]
    timestamp = digipeater[1]
    heard = False
    ssid = None
    timedelta = None

    if '*' in digipeater_call:
        heard = True

    try:
        ssid = int(re.sub(r'[^\w]', ' ', digipeater_call.split('-')[1]))
    except IndexError:
        # No ssid
        ssid = None

    digipeater_call = re.sub(r'[^\w]', ' ',
                             digipeater_call.split('-')[0]).strip().upper()

    try:
        last_seen = session.query(RemoteDigipeater.lastheard). \
            distinct(RemoteDigipeater.call, RemoteDigipeater.lastheard). \
            filter(RemoteDigipeater.call == f'{digipeater_call}'). \
            order_by(RemoteDigipeater.lastheard).first()

        if last_seen:
            last_seen = last_seen[0]
            timedelta = (now - last_seen)

    except IndexError:
        last_seen = None  # New digi
        timedelta = None

    if digipeater_call not in existing_digipeaters_data and \
            digipeater_call not in added_digipeaters:

        digipeater_info = get_info(digipeater_call)

        if digipeater_info:
            print(f"Adding digipeater {digipeater_call}")

            try:
                lat = float(digipeater_info[0])
                lon = float(digipeater_info[1])
                grid = digipeater_info[2]
            except IndexError:
                lat = None
                lon = None
                grid = None

            if lat is not None and lon is not None:
                remote_digi = RemoteDigipeater(parent_call=node_to_crawl,
                                               call=digipeater_call,
                                               lastheard=timestamp,
                                               grid=grid,
                                               heard=heard,
                                               ssid=ssid,
                                               geom=f'SRID=4326;POINT({lon} {lat})',
                                               port=port_name,
                                               uid=f"{node_to_crawl}-{port_name}")

                session.add(remote_digi)
            added_digipeaters.append(digipeater_call)

    elif timedelta and timedelta.days >= refresh_days:
        digipeater_info = get_info(digipeater_call)

        if digipeater_info:
            print(f"Adding digipeater {digipeater_call}")
            lat = float(digipeater_info[0])
            lon = float(digipeater_info[1])
            grid = digipeater_info[2]

            if lat is not None and lon is not None:
                print(f"Updating digipeater coordinates for {digipeater}")
                session.query(RemoteDigipeater).filter(
                    RemoteDigipeater.call == f"{digipeater_call}").update(
                    {RemoteDigipeater.geom: f"SRID=4326;POINT({lon} {lat})"},
                    synchronize_session="fetch")

    # Update timestamp
    if last_seen and last_seen < timestamp:
        session.query(RemoteDigipeater).filter(
            RemoteDigipeater.call == f"{digipeater_call}").update(
            {RemoteDigipeater.lastheard: timestamp},
            synchronize_session="fetch")

# Get bands for each operator
print("Updating bands columns")

case_statement = "UPDATE public.remote_mh SET band = CASE " \
                 "WHEN (port LIKE '%44_.%' OR port LIKE '44_.%') AND port NOT LIKE '% 14.%' AND port NOT LIKE '% 7.%' THEN '70CM' " \
                 "WHEN (port LIKE '%14_.%' OR port LIKE '14_.%') AND port NOT LIKE '% 14.%' AND port NOT LIKE '% 7.%' THEN '2M' " \
                 "WHEN (port LIKE '% 14.%' OR port LIKE '14.%') AND port NOT LIKE '%14_.%' AND port NOT LIKE '% 7.%' THEN '20M' " \
                 "WHEN (port LIKE '% 7.%' OR port LIKE '7.%') AND port NOT LIKE '%14_.%' AND port NOT LIKE '%14.%%%' THEN '40M' " \
                 "END;"
# engine.execute(text(case_statement)).execution_options(autocommit=True)
session.execute(case_statement)

# Populate bands column for remote_operators table
all_operators = session.query(RemoteOperator).filter(
    RemoteOperator.bands.is_(None))

for operator in all_operators:
    operating_bands = ""
    call = operator.remote_call
    call = call.split('-')[0].strip()
    bands = operator.bands

    all_mh = session.query(RemotelyHeardStation).filter(
        RemotelyHeardStation.remote_call.like(f'{call}%'))

    if bands:
        operating_bands += bands

    for mh_item in all_mh:
        remote_call = mh_item.remote_call
        band = mh_item.band

        if remote_call.split('-')[0] == call and band:
            if band not in operating_bands:
                operating_bands += f"{band},"

    if len(operating_bands) > 0:
        session.query(RemoteOperator).filter(
            RemoteOperator.remote_call == f'{call}').update(
            {RemoteOperator.bands: operating_bands},
            synchronize_session="fetch")

# if not debug:
# session.commit()
session.close()
