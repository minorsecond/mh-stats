#!/bin/python3
# Save MH data to PostgreSQL database

import datetime
import re
from telnetlib import Telnet

import psycopg2
from shapely.geometry import Point

from common import get_info, get_conf

refresh_days = 1

conf = get_conf()


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
    query = f"SELECT DISTINCT on (call, lastheard) call, lastheard FROM packet_mh.{table} WHERE call='{call}' ORDER BY lastheard DESC LIMIT 1;"
    cursor.execute(query)
    return cursor.fetchall()


# Connect to PG
con = psycopg2.connect(database=conf['pg_db'], user=conf['pg_user'],
                       password=conf['pg_pw'], host=conf['pg_host'],
                       port=conf['pg_port'])

now = datetime.datetime.now().replace(microsecond=0)
year = datetime.date.today().year

tn = Telnet(conf['telnet_ip'], conf['telnet_port'], timeout=5)
tn.read_until(b"user: ", timeout=2)
tn.write(conf['telnet_user'].encode('ascii') + b"\r")
tn.read_until(b"password:", timeout=2)
tn.write(conf['telnet_pw'].encode('ascii') + b"\r")
tn.read_until(b"Connected", timeout=2)
tn.write("mhu 1".encode('ascii') + b"\r")
tn.write(b"\r")
tn.write(b"bye\r")

print(f"Connected to {conf['telnet_ip']}")

output = tn.read_all()
output = output.split(b"Port 1")[1].strip()
output = output.split(b'***')[0]
output = re.sub(b' +', b' ', output)
output = output.split(b'\r\n')

index = 0
for item in output:
    output[index] = item.strip().split(b' ')
    index += 1

# Clean up list
for index, item in enumerate(output):
    if b'via' in item:
        item.remove(b'via')

    if len(item) == 1:
        del output[index]

radio_mh_list = []
for item in output:
    res = []

    call = item[0].decode('utf-8')
    res.append(call)
    month = item[1].decode('utf-8')
    day = item[2].decode('utf-8')
    time = item[3].decode('utf-8')

    res.append(datetime.datetime.strptime(f"{month} {day} {year} {time}", "%b %d %Y %H:%M:%S"))
    try:
        digipeaters = item[4].decode('utf-8').split(',')
        res.append(digipeaters)
    except IndexError:
        res.append(None)

    radio_mh_list.append(res)

# Write to PG, first get existing data to check for duplicates
read_mh = con.cursor()
read_mh.execute("SELECT * FROM packet_mh.mh_list")
existing_mh = read_mh.fetchall()

existing_mh_data = []
for row in existing_mh:
    call = row[2]
    timestamp = row[1]
    hms = timestamp.strftime("%H:%M:%S")
    existing_mh_data.append(f"{call} {hms}")

radio_mh_list = sorted(radio_mh_list, key=lambda x: x[1], reverse=False)

read_operators = con.cursor()
read_operators.execute("SELECT id, call, ST_X(geom), ST_Y(geom) FROM packet_mh.operators")
existing_ops = read_operators.fetchall()

existing_ops_data = {}
for op in existing_ops:
    call = op[1]
    lon = op[2]
    lat = op[3]
    existing_ops_data[call] = (lat, lon)

read_digipeaters = con.cursor()
read_digipeaters.execute("SELECT call, ST_X(geom), ST_Y(geom), heard FROM packet_mh.digipeaters")
existing_digipeaters = read_digipeaters.fetchall()

existing_digipeaters_data = {}
for digipeater in existing_digipeaters:
    call = digipeater[0]
    lon = digipeater[1]
    lat = digipeater[2]
    heard = digipeater[3]
    existing_digipeaters_data[call] = (lat, lon, heard)

# Write to PG
digipeater_list = {}
current_op_list = []
write_cursor = con.cursor()
for item in radio_mh_list:
    call = item[0].strip()
    op_call = re.sub(r'[^\w]', ' ', call.split('-')[0].strip())
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
        write_cursor.execute(f"INSERT INTO packet_mh.mh_list (timestamp,call,digipeaters,op_call) VALUES ('{timestamp}','{call}','{digipeaters}', '{op_call}')")

    # Update ops last heard
    if last_heard and timestamp > last_heard:
        update_op_query = f"UPDATE packet_mh.operators SET lastheard = '{timestamp}' WHERE call = '{op_call}';"
        write_cursor.execute(update_op_query)

    # Write Ops table if
    if op_call not in existing_ops_data and op_call not in current_op_list:
        # add coordinates & grid
        info = get_info(call.split('-')[0])

        if info:
            lat = float(info[0])
            lon = float(info[1])
            point = Point(lon, lat).wkb_hex
            grid = info[2]

        print(f"{now} Adding {op_call} to operator table.")
        write_cursor.execute(f"INSERT INTO packet_mh.operators (call, lastheard, geom, grid) VALUES ('{op_call}', '{timestamp}', st_setsrid('{point}'::geometry, 4326), '{grid}')")
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
            update_op_query = f"UPDATE packet_mh.operators SET geom = st_setsrid('{point}'::geometry, 4326) WHERE call = '{op_call}';"
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

    digipeater_call = re.sub(r'[^\w]', ' ', digipeater_call.split('-')[0]).strip()

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

            write_cursor.execute("INSERT INTO packet_mh.digipeaters "
                                 "(call, lastheard, grid, geom, heard, ssid) "
                                 "VALUES (%s, %s, %s, "
                                 "st_setsrid(%s::geometry, 4326), %s, %s)",
                                 (digipeater_call, timestamp, grid, point,
                                  heard, ssid))

            added_digipeaters.append(digipeater_call)

    elif timedelta and timedelta.days >= refresh_days:
        digipeater_info = get_info(digipeater_call)

        if digipeater_info:
            print(f"Adding digipeater {digipeater_call}")
            lat = float(digipeater_info[0])
            lon = float(digipeater_info[1])
            point = Point(lon, lat).wkb_hex
            grid = digipeater_info[2]
            print(f"Updating digipeater coordinates for {digipeater}")
            update_op_query = "UPDATE packet_mh.digipeaters SET geom = st_setsrid(%s::geometry, 4326) WHERE call = %s", (
            point, digipeater_call)
            # update_digi_query = f"UPDATE packet_mh.digipeaters SET geom = st_setsrid('{point}'::geometry, 4326) WHERE call = '{digipeater_call}';"
            write_cursor.execute(update_digi_query)

    # Update timestamp
    if last_seen and last_seen < timestamp:
        update_digi_query = f"UPDATE packet_mh.digipeaters SET lastheard = '{timestamp}', heard = '{heard}' WHERE call = '{digipeater_call}';"
        write_cursor.execute(update_digi_query)

con.commit()
con.close()