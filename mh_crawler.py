#!/bin/python3

import argparse
import datetime
import re
from string import digits

from geoalchemy2.shape import to_shape
from sqlalchemy import or_
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql.expression import true

from common import get_info, get_conf, telnet_connect, node_connect, \
    auto_node_selector
from common.string_cleaner import strip_call
from models.db import local_engine, CrawledNode, RemoteOperator, \
    RemoteDigipeater, \
    RemotelyHeardStation, BadGeocode

refresh_days = 1
debug = False

print(f"\n==============================================\n"
      f"Run at {datetime.datetime.utcnow().replace(microsecond=0)}")

port_name = None

parser = argparse.ArgumentParser(description="Crawl BPQ nodes")
parser.add_argument('--node', metavar='N', type=str, help="Node name to crawl")
parser.add_argument('-auto', action='store_true',
                    help="Pick a node to crawl automatically")
parser.add_argument('-v', action='store_true', help='Verbose log')
args = parser.parse_args()
node_to_crawl = args.node
auto = args.auto
verbose = args.v
conf = get_conf()
info_method = conf['info_method']

if node_to_crawl:
    node_to_crawl = node_to_crawl.strip().upper()

year = datetime.date.today().year

# Connect to PG
Session = sessionmaker(bind=local_engine)

last_crawled_port_name = None
node_to_crawl_info = {}
read_crawled_nodes = Session()
session = Session()

if auto and node_to_crawl:
    print("You can't enter node to crawl & auto mode")
    exit()

elif auto and not debug:
    node_to_crawl_info = auto_node_selector(CrawledNode, session, refresh_days)
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

    """
    Split node name into two parts and put into list. This is for checking if
    a bad geocode has already been added to the bad geocode table later in the
    script.
    """
    if ':' in geocode:
        node_part_one = geocode.split(':')[0].split('-')[0]
        node_part_two = geocode.split(':')[1].split('-')[0]
        bad_geocodes.append(node_part_one)
        bad_geocodes.append(node_part_two)
    else:
        bad_geocodes.append(geocode)

# Get all remote operators
existing_ops = session.query(RemoteOperator.remote_call,
                             RemoteOperator.geom).all()

existing_ops_data = {}
for op in existing_ops:
    remote_call = op[0]
    point = to_shape(op[1])
    lon = point.x
    lat = point.y

    existing_ops_data[remote_call] = (lat, lon)

# Get all remote digipeaters
existing_digipeaters = session.query(RemoteDigipeater.call,
                                     RemoteDigipeater.geom,
                                     RemoteDigipeater.heard,
                                     RemoteDigipeater.ports).all()

existing_digipeaters_data = {}
for digipeater in existing_digipeaters:
    digipeater_call = digipeater[0]
    point = to_shape(digipeater[1])
    lon = point.x
    lat = point.y
    heard = digipeater[2]
    ports = digipeater[3]
    existing_digipeaters_data[digipeater_call] = (lat, lon, heard, ports)

# Get all remote MHeard list
existing_remote_mh_results = session.query(RemotelyHeardStation.remote_call,
                                           RemotelyHeardStation.heard_time).all()

existing_mh_data = []
for row in existing_remote_mh_results:
    call = row[0]
    timestamp = row[1]
    hour_minute = timestamp.strftime("%H:%M:%S")
    existing_mh_data.append(f"{call} {hour_minute}")

# Connect to local telnet server
tn = telnet_connect()

if auto and not debug:
    # Get node to crawl from dict
    node_to_crawl = list(node_to_crawl_info.keys())[0]
    print(f"Auto crawling node {node_to_crawl}")

if not debug:  # Stay local if debugging
    try:
        tn = node_connect(node_to_crawl, tn)
    except KeyboardInterrupt:
        print("Closing connection")
        tn.write(b'bye\r')
        exit()

# Get available ports
available_ports = None
tn.write("p".encode('ascii') + b'\r')  # Get available ports
tn.write(b'\n\n\n')

# Print available ports to screen
available_ports_raw = tn.read_until(b'\n\n\n',
                                    timeout=20)

try:
    available_ports_raw = available_ports_raw.split(b"Ports")[1].strip()
    available_ports_raw = available_ports_raw.split(b'***')[0]
    available_ports_raw = re.sub(b' +', b' ', available_ports_raw)
    available_ports = available_ports_raw.split(b'\r\n')
except IndexError:
    # Corrupt data
    print(f"Possible corrupt data received: {available_ports_raw}")
    exit()


node_name_map = {}
for port in available_ports:
    menu_item = int(re.search(r'\d+', port.decode('utf-8')).group())
    port_name = port.decode('utf-8').strip().lstrip(digits).strip()
    node_name_map[menu_item] = port_name

if not auto:
    # Give menu options on screen
    selected_port = None
    menu_item = 1
    print("Select VHF/UHF port to scan MHeard on")
    for menu_item, port_name in node_name_map.items():
        print(f"{menu_item}: {port_name}")

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

    port_name = node_name_map.get(selected_port).strip()

    last_crawled_port_name = session.query(CrawledNode.port_name).filter(
        CrawledNode.port == selected_port,
        CrawledNode.node_id == node_to_crawl,
        CrawledNode.active_port == true()
    ).one_or_none()

    if last_crawled_port_name:
        last_crawled_port_name = last_crawled_port_name[0]

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
        print(f"Port has changed for {node_to_crawl}. "
              f"Was {last_crawled_port_name}, is now {port_name}")
        session.query(CrawledNode).filter(CrawledNode.node_id ==
                                          f'{node_to_crawl}',
                                          CrawledNode.port == selected_port). \
            update({CrawledNode.needs_check: True},
                   synchronize_session='fetch')
        session.commit()
        session.close()
        tn.write(b"bye\r")
        exit()

    # Send the MH command
    print(f"Getting MH list for port {selected_port}.")
    mh_command = f"mh {selected_port}".encode('ascii')
    now = datetime.datetime.utcnow().replace(microsecond=0)
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
        call = item[0].decode('utf-8').strip()
        res.append(call)
        time_passed = item[1].decode('utf-8')

        try:
            days_passed = int(time_passed.split(':')[0])
            hours_passed = int(time_passed.split(':')[1])
            minutes_passed = int(time_passed.split(':')[2])
            seconds_passed = int(time_passed.split(':')[3])
        except ValueError or IndexError as v:
            print(f"Error trying to parse time: {v}")
            exit()

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
            digipeaters = item[2].decode('utf-8').split(',')
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

    if verbose:
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

elif not auto:  # Write new node
    # Get port crawled by node name, port number and port name
    crawled_nodes = session.query(CrawledNode).filter(
        CrawledNode.node_id == node_to_crawl,
        CrawledNode.port == selected_port,
        CrawledNode.port_name == port_name).one_or_none()

    if crawled_nodes:
        nodes_to_crawl_id = crawled_nodes.id

        session.query(CrawledNode).filter(
            CrawledNode.id == nodes_to_crawl_id).update(
            {CrawledNode.last_crawled: now}, synchronize_session="fetch")

        # Populate needs check field if null
        session.query(CrawledNode).filter(CrawledNode.id == nodes_to_crawl_id,
                                          CrawledNode.needs_check.is_(None)). \
            update({CrawledNode.needs_check: False,
                    CrawledNode.active_port: True},
                   synchronize_session="fetch")

        # Update the port name if it's empty
        if selected_port and node_to_crawl and selected_port and \
                last_crawled_port_name is None:
            if verbose:
                print(f"Adding port name {port_name} to existing row")
            session.query(CrawledNode).filter(
                CrawledNode.id == nodes_to_crawl_id).update(
                {CrawledNode.port_name: port_name,
                 CrawledNode.last_crawled: now}, synchronize_session="fetch")

        """
        If no results from query above, that means this is either a new node, 
        new port, or that the port name has changed. In this case, we write a 
        new row to the table with the new port information.
        """
    elif not crawled_nodes and selected_port and node_to_crawl:
        if verbose:
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
mh_counter = 0
new_ops_counter = 0
bad_geocodes_counter = 0
updated_ops_counter = 0

for item in mh_list:
    time_diff = None
    info = None

    call, op_call, ssid = strip_call(item[0])
    input(op_call)

    timestamp = item[1]

    # Get last time station was heard
    try:
        last_heard = session.query(RemoteOperator.lastheard). \
            distinct(RemoteOperator.remote_call, RemoteOperator.lastheard). \
            filter(RemoteOperator.remote_call == f'{op_call}'). \
            order_by(RemoteOperator.lastheard.desc()).first()

        last_check = session.query(RemoteOperator.lastcheck).\
            distinct(RemoteOperator.remote_call, RemoteOperator.lastcheck).\
            filter(RemoteOperator.remote_call == f'{op_call}').\
            order_by(RemoteOperator.lastcheck.desc()).first()

        if last_heard:
            last_heard = last_heard[0]

        if last_check is not None:
            last_check = last_check[0]

            if last_check is not None:
                time_diff = (now - last_check)
            else:
                time_diff = None

        else:
            time_diff = None

    except IndexError:
        time_diff = None
        last_heard = None

    times = [(timestamp + datetime.timedelta(seconds=x)).strftime("%H:%M:%S")
             for x in range(-5, 5)]

    check_list = []
    to_add = True
    for time in times:
        if f"{call} {time}" in existing_mh_data:
            to_add = False

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
    if to_add is True:
        if verbose:
            print(f"{now} Adding {call} at {timestamp} through {digipeaters}.")

        remotely_heard = RemotelyHeardStation(
            parent_call=node_to_crawl,
            remote_call=call,
            heard_time=timestamp,
            ssid=ssid,
            update_time=now,
            port=port_name,
            uid=f"{node_to_crawl}-{port_name}",
            digis=digipeaters
        )

        session.add(remotely_heard)
        mh_counter += 1

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
                info = get_info(call.split('-')[0], info_method)
            except Exception as e:
                print(f"Error {e} on {call}")

        else:
            try:
                info = get_info(call, info_method)
            except Exception as e:
                print(f"Error {e} on {call}")

        if info:
            try:
                lat = float(info[0])
                lon = float(info[1])
                grid = info[2]
            except ValueError:
                if verbose:
                    print(f"Couldn't get coordinates for {op_call}")
                grid = None

        if grid:  # No grid means no geocode generally
            if verbose:
                print(f"{now} Adding {op_call} to operator table.")

            remote_operator = RemoteOperator(
                parent_call=node_to_crawl,
                remote_call=op_call,
                lastheard=timestamp,
                grid=grid,
                geom=f'SRID=4326;POINT({lon} {lat})',
                port=port_name,
                uid=f"{node_to_crawl}-{port_name}",
                lastcheck=now
            )

            session.add(remote_operator)
            new_ops_counter += 1

        else:  # Add to bad_geocodes table
            if op_call not in bad_geocodes:
                if verbose:
                    print(
                        f"{op_call} not geocoded. Adding to bad geocode table")
                new_bad_geocode = BadGeocode(
                    last_checked=now,
                    reason="Operator not geocoded",
                    node_name=op_call,
                    parent_node=node_to_crawl
                )

                session.add(new_bad_geocode)
                bad_geocodes_counter += 1

    elif op_call not in current_op_list:  # Update existing op
        if time_diff is None or time_diff.days >= refresh_days:
            # add coordinates & grid
            info = get_info(call.split('-')[0], info_method)

            if info:
                try:
                    lat = float(info[0])
                    lon = float(info[1])
                    grid = info[2]
                except IndexError:
                    lat = None
                    lon = None
                    grid = None

                if verbose:
                    print(f"Updating coordinates for {op_call}")
                if lat is not None and lon is not None:
                    session.query(RemoteOperator).filter(
                        RemoteOperator.remote_call == f'{op_call}').update(
                        {RemoteOperator.parent_call: node_to_crawl,
                         RemoteOperator.geom: f"SRID=4326;POINT({lon} {lat})",
                         RemoteOperator.grid: grid,
                         RemoteOperator.port: port_name,
                         RemoteOperator.uid: f"{node_to_crawl}-{port_name}",
                         RemoteOperator.lastcheck: now},
                        synchronize_session="fetch")
                    updated_ops_counter += 1

        else:  # Update port & uid
            if verbose:
                print(f"Updating parent node & port data for {op_call}")
            session.query(RemoteOperator).filter(
                RemoteOperator.remote_call == f'{op_call}').update(
                {RemoteOperator.parent_call: node_to_crawl,
                 RemoteOperator.port: port_name,
                 RemoteOperator.uid: f"{node_to_crawl}-{port_name}"},
                synchronize_session="fetch")
            updated_ops_counter += 1

    current_op_list.append(op_call)

# Write digipeaters table
added_digipeaters = []
new_digipeater_counter = 0
updated_digipeater_counter = 0
for digipeater in digipeater_list.items():
    lat = None
    lon = None
    grid = None
    digipeater_call = digipeater[0]
    timestamp = digipeater[1]
    heard = False
    ssid = None
    time_diff = None

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

        last_check = session.query(RemoteDigipeater.lastcheck).\
            distinct(RemoteDigipeater.call, RemoteDigipeater.lastcheck).\
            filter(RemoteDigipeater.call == f'{digipeater_call}').\
            order_by(RemoteDigipeater.lastcheck).first()

        if last_seen:
            last_seen = last_seen[0]

        if last_check is not None:
            last_check = last_check[0]

            if last_check is not None:
                time_diff = (now - last_check)
            else:
                time_diff = None

        else:
            time_diff = None

    except IndexError:
        last_seen = None  # New digi
        time_diff = None

    # Add new digipeater
    if digipeater_call not in existing_digipeaters_data and \
            digipeater_call not in added_digipeaters:

        digipeater_info = get_info(digipeater_call, info_method)

        if digipeater_info:
            if verbose:
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
                                               last_port=port_name,
                                               uid=f"{node_to_crawl}-{port_name}",
                                               ports=port_name,
                                               lastcheck=now)

                session.add(remote_digi)
                new_digipeater_counter += 1
            added_digipeaters.append(digipeater_call)
        elif verbose:
            print(f"Could not get info for digipeater: {digipeater_call}")

    else:
        if digipeater_call in existing_digipeaters_data:
            # Update last port and ssid and parent call
            if verbose:
                print(
                    f"Updating parent node, ssid, and port name for {digipeater_call}")
            session.query(RemoteDigipeater). \
                filter(RemoteDigipeater.call == f"{digipeater_call}"). \
                update({RemoteDigipeater.parent_call: node_to_crawl,
                        RemoteDigipeater.last_port: port_name,
                        RemoteDigipeater.ssid: ssid})
        if time_diff is None or time_diff.days >= refresh_days:
            digipeater_info = get_info(digipeater_call, info_method)

            if digipeater_info:
                if verbose:
                    print(f"Adding digipeater {digipeater_call}")
                lat = float(digipeater_info[0])
                lon = float(digipeater_info[1])
                grid = digipeater_info[2]

                if lat is not None and lon is not None:
                    if verbose:
                        print(
                            f"Updating digipeater coordinates for {digipeater}")
                    session.query(RemoteDigipeater).\
                        filter(RemoteDigipeater.call == f"{digipeater_call}").\
                        update({
                        RemoteDigipeater.geom: f"SRID=4326;POINT({lon} {lat})",
                        RemoteDigipeater.lastcheck: now},
                        synchronize_session="fetch")
                    updated_digipeater_counter += 1

            # Add new digipeater port
    if digipeater_call in existing_digipeaters_data and \
            digipeater_call not in added_digipeaters:
        existing_digi_ports = \
            existing_digipeaters_data.get(digipeater_call)[3]

        port_list = None
        if not existing_digi_ports:
            port_list = port_name
        else:
            port_list = existing_digi_ports
            if port_name not in port_list:
                port_list += ',' + port_name

        if not existing_digi_ports or port_name not in existing_digi_ports:
            session.query(RemoteDigipeater). \
                filter(RemoteDigipeater.call == digipeater_call). \
                update({RemoteDigipeater.ports: port_list})

    # Update timestamp
    if last_seen and last_seen < timestamp:
        if verbose:
            print(
                f"Updating timestamp for digipeater {digipeater_call} with TS {timestamp}")
        session.query(RemoteDigipeater).filter(
            RemoteDigipeater.call == f"{digipeater_call}").update(
            {RemoteDigipeater.lastheard: timestamp},
            synchronize_session="fetch")

    # Add to port list

# Get bands for each operator

if verbose:
    print("Updating bands columns")
case_statement = "UPDATE public.remote_mh SET band = CASE " \
                 "WHEN (port LIKE '%4__.%' OR port LIKE '44_.%') AND port NOT LIKE '% 14.%' AND port NOT LIKE '% 7.%' THEN '70CM' " \
                 "WHEN (port LIKE '%14_.%' OR port LIKE '14_.%') AND port NOT LIKE '% 14.%' AND port NOT LIKE '% 7.%' THEN '2M' " \
                 "WHEN (port LIKE '%22_.%' OR port LIKE '22_.%') THEN '1.25M' " \
                 "WHEN (port LIKE '% 14.%' OR port LIKE '14.%') AND port NOT LIKE '%14_.%' AND port NOT LIKE '% 7.%' THEN '20M' " \
                 "WHEN (port LIKE '% 7.%' OR port LIKE '7.%') AND port NOT LIKE '%14_.%' AND port NOT LIKE '%14.%%%' THEN '40M' " \
                 "WHEN (port LIKE '% 3.%' OR port LIKE '3.%') AND port NOT LIKE '%14_.%' AND port NOT LIKE '%14.%%%' THEN '80M' " \
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

        if re.sub('[\W_]+', '', remote_call.split('-')[0]) == call and band:
            if band not in operating_bands:
                operating_bands += f"{band},"

    if len(operating_bands) > 0:
        session.query(RemoteOperator).filter(
            RemoteOperator.remote_call == f'{call}').update(
            {RemoteOperator.bands: operating_bands},
            synchronize_session="fetch")

if not debug:
    session.commit()
session.close()

print(f"{now} - Added {mh_counter} MH rows, with {new_ops_counter} "
      f"new operators, {updated_ops_counter} updated ops, and "
      f"{bad_geocodes_counter} bad geocodes. Added {new_digipeater_counter} "
      f"new digipeaters and updated {updated_digipeater_counter} digipeaters.")
