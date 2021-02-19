#!/bin/python3

import argparse
import datetime
import re
from string import digits
from telnetlib import Telnet

import psycopg2
from shapely.geometry import Point

from common import get_info, get_conf

refresh_days = 14
debug = False


def get_last_heard(call, type):
    """
    Get last time station was heard
    :param call: Callsign
    :param type: Digipeater or Node
    :return: timestamp
    """

    cursor = con.cursor()
    table = None
    if type == "node":
        table = "operators"
    elif type == "digi":
        table = "digipeaters"

    # noinspection SqlResolve
    query = f"SELECT DISTINCT on (call, lastheard) call, lastheard FROM public.{table} WHERE call='{call}' ORDER BY lastheard DESC LIMIT 1;"
    cursor.execute(query)
    return cursor.fetchall()


parser = argparse.ArgumentParser(description="Crawl BPQ nodes")
parser.add_argument('--node', metavar='N', type=str, help="Node name to crawl")
parser.add_argument('-auto', action='store_true',
                    help="Pick a node to crawl automatically")
args = parser.parse_args()
node_to_crawl = args.node
auto = args.auto
conf = get_conf()

now = datetime.datetime.now().replace(microsecond=0)
year = datetime.date.today().year

# Connect to PG
con = psycopg2.connect(database=conf['pg_db'], user=conf['pg_user'],
                       password=conf['pg_pw'], host=conf['pg_host'],
                       port=conf['pg_port'])

last_crawled_port_name = None
node_to_crawl_info = {}
read_crawled_nodes = con.cursor()
if auto and node_to_crawl:
    print("You can't enter node to crawl & auto mode")
    exit()

elif auto and not debug:
    # Get a node that hasn't been crawled in 2 weeks
    read_crawled_nodes.execute(
        f"SELECT id, node_id, port, last_crawled, port_name FROM crawled_nodes WHERE last_crawled < now() - INTERVAL '3 hours' ORDER BY random() LIMIT 1"
    )
    crawled_node = read_crawled_nodes.fetchall()[0]
    if len(crawled_node) > 0:
        node_to_crawl_info = {
            crawled_node[1]: (
                crawled_node[0], crawled_node[2], crawled_node[3],
                crawled_node[4])
        }
        last_crawled_port_name = crawled_node[4]
    else:
        print("Nothing to crawl")
        exit()

elif not node_to_crawl and not debug:
    print("You must enter a node to crawl.")
    exit()
elif not node_to_crawl and not auto:
    node_to_crawl = "KD5LPB"

# Get all remote operators
read_operators = con.cursor()
read_operators.execute(
    "SELECT id, parent_call, remote_call,lastheard,grid, ST_X(geom), ST_Y(geom) FROM public.remote_operators")
existing_ops = read_operators.fetchall()

existing_ops_data = {}
for op in existing_ops:
    remote_call = op[2]
    lon = op[5]
    lat = op[6]
    existing_ops_data[remote_call] = (lat, lon)

# Get all remote digipeaters
read_digipeaters = con.cursor()
read_digipeaters.execute(
    "SELECT parent_call, call, lastheard, grid, heard, ssid, ST_X(geom), ST_Y(geom) FROM public.remote_digipeaters")
existing_digipeaters = read_digipeaters.fetchall()

existing_digipeaters_data = {}
for digipeater in existing_digipeaters:
    digipeater_call = digipeater[1]
    lon = digipeater[6]
    lat = digipeater[7]
    heard = digipeater[4]
    existing_digipeaters_data[digipeater_call] = (lat, lon, heard)

# Get remote MHeard list
remote_mh_cursor = con.cursor()
remote_mh_cursor.execute(
    'SELECT parent_call, remote_call, heard_time, update_time FROM public.remote_mh')
existing_remote_mh_results = remote_mh_cursor.fetchall()

existing_mh_data = []
for row in existing_remote_mh_results:
    call = row[1]
    timestamp = row[2]
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
        con_results = tn.read_until(b'Connected to', timeout=20)

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
        'utf-8').strip().lstrip(digits)

    # Exit if port has changed
    if last_crawled_port_name and port_name.strip() != last_crawled_port_name.strip():
        print(f"Port has changed for {node_to_crawl}")
        read_crawled_nodes.execute(
            f"UPDATE crawled_nodes SET needs_check='True' WHERE node_id='{node_to_crawl}'")
        exit()

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
    if len(item) == 4:
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

# Write to PG
digipeater_list = {}
current_op_list = []
write_cursor = con.cursor()
for item in mh_list:
    info = None
    call = item[0].strip()
    op_call = re.sub(r'[^\w]', ' ', call.split('-')[0].strip())

    if '-' in call:
        ssid = re.sub(r'[^\w]', ' ',
                      call.split('-')[1])
    else:
        ssid = None

    timestamp = item[1]

    try:
        last_heard = get_last_heard(op_call, "node")[0][1]
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

        write_cursor.execute("INSERT INTO public.remote_mh "
                             "(parent_call, remote_call, heard_time, ssid, update_time, port) "
                             "VALUES (%s, %s, %s, %s, %s, %s)",
                             (node_to_crawl, call, timestamp, ssid, now,
                              port_name))

    # Update ops last heard
    if last_heard and timestamp > last_heard:
        update_op_query = f"UPDATE public.remote_operators SET lastheard = '{timestamp}' WHERE remote_call = '{op_call}';"
        write_cursor.execute(update_op_query)

    # Write Ops table if
    if op_call not in existing_ops_data and op_call not in current_op_list:
        # add coordinates & grid
        if '-' in call:
            try:
                info = get_info(call.split('-')[0])
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
            write_cursor.execute("INSERT INTO public.remote_operators "
                                 "(parent_call, remote_call, lastheard, grid, geom, port) "
                                 "VALUES (%s, %s, %s, %s, st_setsrid(%s::geometry, 4326), %s)",
                                 (node_to_crawl, op_call, timestamp, grid,
                                  point, port_name))
        current_op_list.append(op_call)

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
                update_op_query = f"UPDATE public.remote_operators SET geom = st_setsrid('{point}'::geometry, 4326) WHERE remote_call = '{op_call}';"
                write_cursor.execute(update_op_query)

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
        ssid = re.sub(r'[^\w]', ' ', digipeater_call.split('-')[1])
    except IndexError:
        # No ssid
        ssid = None

    digipeater_call = re.sub(r'[^\w]', ' ',
                             digipeater_call.split('-')[0]).strip()

    try:
        last_seen = get_last_heard(digipeater_call, "digi")[0][1]
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
            point = Point(lon, lat).wkb_hex
            grid = digipeater_info[2]

            write_cursor.execute("INSERT INTO public.remote_digipeaters "
                                 "(parent_call, call, lastheard, grid, heard, ssid, geom, port) "
                                 "VALUES (%s, %s, %s, "
                                 "%s, %s, %s, st_setsrid(%s::geometry, 4326), %s)",
                                 (node_to_crawl, digipeater_call, timestamp,
                                  grid, heard, ssid, point, port_name))

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
                update_op_query = "UPDATE public.remote_digipeaters SET geom = st_setsrid(%s::geometry, 4326) WHERE call = %s", (
                    point, digipeater_call)
                write_cursor.execute(update_digi_query)

    # Update timestamp
    if last_seen and last_seen < timestamp:
        update_digi_query = f"UPDATE public.remote_digipeaters SET lastheard = '{timestamp}', heard = '{heard}' WHERE call = '{digipeater_call}';"
        write_cursor.execute(update_digi_query)

# Get bands for each operator
print("Updating bands columns")
write_cursor.execute("UPDATE public.remote_mh SET band = CASE "
                     "WHEN (port LIKE '%44_.%' OR port LIKE '44_.%') AND port NOT LIKE '% 14.%' AND port NOT LIKE '% 7.%' THEN '70CM' "
                     "WHEN (port LIKE '%14_.%' OR port LIKE '14_.%') AND port NOT LIKE '% 14.%' AND port NOT LIKE '% 7.%' THEN '2M' "
                     "WHEN (port LIKE '% 14.%' OR port LIKE '14.%') AND port NOT LIKE '%14_.%' AND port NOT LIKE '% 7.%' THEN '20M' "
                     "WHEN (port LIKE '% 7.%' OR port LIKE '7.%') AND port NOT LIKE '%14_.%' AND port NOT LIKE '%14.%%%' THEN '40M' "
                     "END;")

all_ops_cusror = con.cursor()
all_mh_cursor = con.cursor()
all_ops_cusror.execute(
    "SELECT remote_call, bands from public.remote_operators WHERE bands IS NULL;"
)
all_operators = all_ops_cusror.fetchall()
for operator in all_operators:
    operating_bands = ""
    call = operator[0]
    call = call.split('-')[0]
    bands = operator[1]

    all_mh_cursor.execute(
        f"SELECT remote_call, band FROM public.remote_mh WHERE remote_call LIKE '{call}%'")
    all_mh = all_mh_cursor.fetchall()

    if bands:
        operating_bands += bands

    for mh_item in all_mh:
        remote_call = mh_item[0]
        band = mh_item[1]

        if remote_call.split('-')[0] == call and band:
            if band not in operating_bands:
                operating_bands += f"{band},"

    if len(operating_bands) > 0:
        all_ops_cusror.execute(
            f"UPDATE public.remote_operators SET bands='{operating_bands}' WHERE remote_call = '{call}';")

# Update the nodes crawled table
update_crawled_node_cursor = con.cursor()
if auto and not debug:
    # Update timestamp of crawled node
    # crawled_node[1]: (crawled_node[0], crawled_node[2], crawled_node[3])

    print("Updating crawled node timestamp")
    node_info = node_to_crawl_info.get(node_to_crawl)
    id = node_info[0]
    port = node_info[1]
    timestamp = node_info[2]

    update_crawled_node_query = f"UPDATE crawled_nodes SET last_crawled = '{now}' WHERE id = '{id}'"
    update_crawled_node_cursor.execute(update_crawled_node_query)

    if not last_crawled_port_name:  # Update port name if doesn't exist
        update_crawled_node_query = f"UPDATE crawled_nodes SET port_name = '{port_name}' WHERE id = '{id}'"
        update_crawled_node_cursor.execute(update_crawled_node_query)

elif not debug:  # Write new node
    read_crawled_nodes.execute(
        f"SELECT id, node_id, port, last_crawled FROM crawled_nodes WHERE node_id='{node_to_crawl}'"
    )
    crawled_nodes = read_crawled_nodes.fetchall()

    existing_ports = []
    if len(crawled_nodes) > 0:
        for node in crawled_nodes:
            existing_ports.append(node[2])

    # Add new item to dictionary
    if selected_port and node_to_crawl and selected_port not in existing_ports:
        print(f"Adding {node_to_crawl} to crawled nodes table")
        update_crawled_node_cursor.execute(
            "INSERT INTO crawled_nodes (node_id, port, last_crawled, port_name, needs_check) VALUES (%s, %s, %s, %s, %s)",
            (node_to_crawl, selected_port, now, port_name, False))

con.commit()
con.close()
