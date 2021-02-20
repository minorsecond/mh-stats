#!/bin/python3

import argparse
import datetime
import re
from string import digits
from telnetlib import Telnet

import psycopg2
from geoalchemy2.shape import to_shape
from shapely.geometry import Point
from sqlalchemy import func
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.exc import NoResultFound

from common import get_info, get_conf
from models.db import engine, CrawledNode, RemoteOperator, RemoteDigipeater, \
    RemotelyHeardStation

refresh_days = 14
debug = True

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

now = datetime.datetime.now().replace(microsecond=0)
year = datetime.date.today().year

# Connect to PG
Session = sessionmaker(bind=engine)
con = psycopg2.connect(database=conf['pg_db'], user=conf['pg_user'],
                       password=conf['pg_pw'], host=conf['pg_host'],
                       port=conf['pg_port'])

last_crawled_port_name = None
node_to_crawl_info = {}
read_crawled_nodes = Session()
session = Session()

if auto and node_to_crawl:
    print("You can't enter node to crawl & auto mode")
    exit()

elif auto and not debug:
    twelve_hours_ago = now - datetime.timedelta(hours=12)
    # Get a node that hasn't been crawled in 2 weeks

    try:
        crawled_nodes = session.query(CrawledNode).filter(
            CrawledNode.last_crawled < twelve_hours_ago).order_by(
            func.random()).limit(1).one()
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

    # Update needs_check flag and exit if port has changed
    if last_crawled_port_name and port_name.strip() != last_crawled_port_name.strip():
        print(f"Port has changed for {node_to_crawl}")
        session.query(CrawledNode).filter(CrawledNode.node_id ==
                                          f'{node_to_crawl}'). \
            update({CrawledNode.needs_check: True},
                   synchronize_session='fetch')
        exit()

    # Send the MHU command
    print(f"Getting MH list for port {selected_port}.")
    mh_command = f"mhu {selected_port}".encode('ascii')
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
    if len(item) > 0:
        res = []
        call = item[0].decode('utf-8')
        res.append(call)
        month = item[1].decode('utf-8')
        day = item[2].decode('utf-8')
        time = item[3].decode('utf-8')
        try:
            res.append(
                datetime.datetime.strptime(f"{month} {day} {year} {time}",
                                           "%b %d %Y %H:%M:%S"))
        except ValueError:
            # Got bad time format
            print(f"Error parsing timestamp: {month}-{day}-{year} {time}")
            exit()
        try:
            digipeaters = item[4].decode('utf-8').split(',')
            res.append(digipeaters)
        except IndexError:
            res.append(None)

        mh_list.append(res)

mh_list = sorted(mh_list, key=lambda x: x[1], reverse=False)

# Do the MH List Processing
digipeater_list = {}
current_op_list = []
write_cursor = con.cursor()
for item in mh_list:
    timedelta = None
    info = None

    # Call includes ssid, ie KD5LPB-7
    call = item[0].strip()
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
    point = None
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
            port=port_name
        )

        session.add(remotely_heard)

    # Update ops last heard
    if last_heard and timestamp > last_heard:
        session.query(RemoteOperator). \
            filter(RemoteOperator.remote_call == f"{op_call}"). \
            update({RemoteOperator.lastheard: timestamp},
                   synchronize_session="fetch")

    # Write Ops table if
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
                point = Point(lon, lat).wkb_hex
                grid = info[2]
            except ValueError:
                print(f"Couldn't get coordinates for {op_call}")
                point = None
                grid = None

        if grid:  # No grid means no geocode generally
            print(f"{now} Adding {op_call} to operator table.")

            remote_operator = RemoteOperator(
                parent_call=node_to_crawl,
                remote_call=op_call,
                lastheard=timestamp,
                grid=grid,
                geom=f'SRID=4326;POINT({lon} {lat})',
                port=port_name
            )

            session.add(remote_operator)
        current_op_list.append(op_call)

    # If operator hasn't been scanned in past refresh_days, update info
    elif timedelta and timedelta.days >= refresh_days and op_call not in current_op_list:
        # add coordinates & grid

        info = get_info(call.split('-')[0])

        if info:
            lat = float(info[0])
            lon = float(info[1])
            point = Point(lon, lat).wkb_hex
            grid = info[2]

        if (lat, lon, grid) != existing_ops_data.get(call):
            print(f"Updating coordinates for {op_call}")
            if point:
                session.query(RemoteOperator).filter(
                    RemoteOperator.remote_call == f'{op_call}').update(
                    {RemoteOperator.geom: f"SRID=4326;POINT({lon} {lat})"})

        current_op_list.append(op_call)

# Write digipeaters table
added_digipeaters = []
for digipeater in digipeater_list.items():
    lat = None
    lon = None
    grid = None
    point = None
    digipeater_call = digipeater[0]
    timestamp = digipeater[1]
    heard = False
    ssid = None

    if '*' in digipeater_call:
        heard = True

    try:
        ssid = int(re.sub(r'[^\w]', ' ', digipeater_call.split('-')[1]))
    except IndexError:
        # No ssid
        ssid = None

    digipeater_call = re.sub(r'[^\w]', ' ',
                             digipeater_call.split('-')[0]).strip()

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
            lat = float(digipeater_info[0])
            lon = float(digipeater_info[1])
            grid = digipeater_info[2]

            remote_digi = RemoteDigipeater(parent_call=node_to_crawl,
                                           call=digipeater_call,
                                           lastheard=timestamp,
                                           grid=grid,
                                           heard=heard,
                                           ssid=ssid,
                                           geom=f'SRID=4326;POINT({lon} {lat})',
                                           port=port_name)

            session.add(remote_digi)
            added_digipeaters.append(digipeater_call)

    elif timedelta and timedelta.days >= refresh_days:
        digipeater_info = get_info(digipeater_call)

        if digipeater_info:
            print(f"Adding digipeater {digipeater_call}")
            lat = float(digipeater_info[0])
            lon = float(digipeater_info[1])
            point = Point(lon, lat).wkb_hex
            grid = digipeater_info[2]

            if point:
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
    call = call.split('-')[0]
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

# Update the nodes crawled table
update_crawled_node_cursor = con.cursor()
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

    if not last_crawled_port_name:  # Update port name if doesn't exist
        session.query(CrawledNode).filter(
            CrawledNode.id == nodes_to_crawl_id).update(
            {CrawledNode.port_name: port_name}, synchronize_session="fetch")

elif not debug:  # Write new node
    crawled_nodes = session.query(CrawledNode).filter(
        CrawledNode.node_id == node_to_crawl,
        CrawledNode.port == selected_port).one()

    if crawled_nodes:
        nodes_to_crawl_id = crawled_nodes.id

        if selected_port and node_to_crawl and selected_port and last_crawled_port_name is None:
            print("Adding port name to existing row")
            session.query(CrawledNode).filter(
                CrawledNode.id == nodes_to_crawl_id).update(
                {CrawledNode.port_name: port_name,
                 CrawledNode.last_crawled: now}, synchronize_session="fetch")

    # Add new item to table
    elif len(crawled_nodes) == 0 and selected_port and node_to_crawl:
        print(f"Adding {node_to_crawl} to crawled nodes table")
        new_crawled_node = CrawledNode(
            node_id=node_to_crawl,
            port=selected_port,
            last_crawled=now,
            port_name=port_name,
            needs_check=False
        )
        session.add(new_crawled_node)

    else:
        print(f"Something bad happened. Crawled node results: {crawled_nodes}")

session.commit()
if not debug:
    con.commit()
con.close()
