#!/bin/python3
# Save MH data to PostgreSQL database

import argparse
import datetime
import re
import time

from sqlalchemy import func, desc
from sqlalchemy.orm import sessionmaker

from common import get_info, get_conf, telnet_connect
from models.db import local_engine, LocallyHeardStation, Operator, \
    Digipeater

parser = argparse.ArgumentParser(description="Scrape BPQ node")
parser.add_argument('-v', action='store_true', help="Verbose logs")
args = parser.parse_args()
verbose = args.v

refresh_days = 7

conf = get_conf()
info_method = conf['info_method']

# Connect to PG
Session = sessionmaker(bind=local_engine)
session = Session()

# Connect to PG

now = datetime.datetime.utcnow().replace(microsecond=0)
year = datetime.date.today().year

print(f"\n==============================================\n"
      f"Run started at {now}")

tn = telnet_connect()
tn.write("mhu 1".encode('ascii') + b"\r")
time.sleep(1)
tn.write(b"\r")
time.sleep(1)
tn.write(b"bye\r")

print(f"Connected to {conf['telnet_ip']}")

output = tn.read_all()
output = output.split(b"Port 1")[1].strip()
output = output.split(b'***')[0]
output = re.sub(b' +', b' ', output)
output = output.split(b'\r\n')

lines = output[0].split(b'\r')
output = []

for item in lines:
    substring = item.strip().split(b'\n')[0]
    sublist = substring.split()
    output.append(sublist)

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

    ymd = datetime.datetime.strptime(f"{month} {day} {year} {time}",
                                     "%b %d %Y %H:%M:%S")
    if ymd > datetime.datetime.utcnow():
        ymd = datetime.datetime.strptime(f"{month} {day} {year - 1} {time}",
                                         "%b %d %Y %H:%M:%S")
    res.append(ymd)
    try:
        digipeaters = item[4].decode('utf-8').split(',')
        res.append(digipeaters)
    except IndexError:
        res.append(None)

    radio_mh_list.append(res)

# Write to PG, first get existing data to check for duplicates
existing_mh = session.query(LocallyHeardStation.call,
                            LocallyHeardStation.timestamp).all()

existing_mh_data = []
for row in existing_mh:
    call = row.call
    timestamp = row.timestamp
    hms = timestamp.strftime("%H:%M:%S")
    existing_mh_data.append(f"{call} {hms}")

radio_mh_list = sorted(radio_mh_list, key=lambda x: x[1], reverse=False)

existing_ops = session.query(Operator.id, Operator.call,
                             func.st_x(Operator.geom),
                             func.st_y(Operator.geom),
                             Operator.grid).all()

existing_ops_data = {}
for op in existing_ops:
    call = op[1]
    lon = op[2]
    lat = op[3]
    grid = op[4]
    existing_ops_data[call] = (lat, lon, grid)

existing_digipeaters = session.query(Digipeater.call,
                                     func.st_x(Digipeater.geom),
                                     func.st_y(Digipeater.geom),
                                     Digipeater.heard).all()

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
mh_counter = 0
new_op_counter = 0
for item in radio_mh_list:
    call = item[0].strip().upper()
    op_call = re.sub(r'[^\w]', ' ', call.split('-')[0].strip())

    try:
        ssid = re.sub(r'[^\w]', ' ', call.split('-')[1].strip())
        ssid = int(ssid)
    except IndexError or TypeError:
        ssid = None

    timestamp = item[1]

    # Get last check time for determining if we should get a new geocode
    try:
        last_heard = session.query(Operator.lastheard). \
            filter(Operator.call == op_call). \
            order_by(desc(Operator.lastheard)).first()

        last_check = session.query(Operator.lastcheck).\
            filter(Operator.call == op_call).\
            order_by(desc(Operator.lastcheck)).first()

        if last_heard is not None:
            last_heard = last_heard[0]

        if last_check is not None:
            last_check = last_check[0]

        if last_check is not None:
            timedelta = (now - last_check)
        else:
            timedelta = None
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
        if verbose:
            print(f"{now} Adding {call} at {timestamp} through {digipeaters}.")

        new_mh_entry = LocallyHeardStation(
            timestamp=timestamp,
            call=call,
            digipeaters=digipeaters,
            op_call=op_call,
            ssid=ssid
        )
        session.add(new_mh_entry)
        mh_counter += 1

    # Update ops last heard
    if last_heard and timestamp > last_heard:
        session.query(Operator).filter(Operator.call == op_call).update(
            {Operator.lastheard: timestamp}, synchronize_session="fetch")

    # Write Ops table if
    if op_call not in existing_ops_data and op_call not in current_op_list:
        # add coordinates & grid
        info = get_info(call.split('-')[0], info_method)

        if info:
            lat = float(info[0])
            lon = float(info[1])
            grid = info[2]

            if verbose:
                print(f"{now} Adding {op_call} to operator table.")
            new_operator = Operator(
                call=op_call,
                lastheard=timestamp,
                geom=f'SRID=4326;POINT({lon} {lat})',
                grid=grid,
                lastcheck=now
            )
            session.add(new_operator)
            current_op_list.append(op_call)
            new_op_counter += 1

    elif timedelta is None or timedelta.days >= refresh_days and op_call not in \
            current_op_list:
        # add coordinates & grid

        info = get_info(call.split('-')[0], info_method)

        if info:
            lat = float(info[0])
            lon = float(info[1])
            grid = info[2]

        if (lat, lon, grid) != existing_ops_data.get(call):
            if verbose:
                print(f"Updating coordinates for {op_call}")
            session.query(Operator).filter(Operator.call == op_call).update(
                {Operator.geom: f'SRID=4326;POINT({lon} {lat})',
                 Operator.lastheard: timestamp,
                 Operator.grid: grid,
                 Operator.lastcheck: now},
                synchronize_session="fetch")

        current_op_list.append(op_call)

# Write digipeaters table
added_digipeaters = []
digipeater_counter = 0
for digipeater in digipeater_list.items():
    lat = None
    lon = None
    grid = None
    digipeater_call = digipeater[0]
    timestamp = digipeater[1]
    heard = False
    ssid = None

    if '*' in digipeater_call:
        heard = True

    try:
        ssid = re.sub(r'[^\w]', ' ', digipeater_call.split('-')[1])
        ssid = int(ssid)
    except IndexError or TypeError:
        # No ssid
        ssid = None

    digipeater_call = re.sub(r'[^\w]', ' ', digipeater_call.split('-')[0]). \
        strip()

    try:
        last_seen = session.query(Digipeater.lastheard).filter(
            Digipeater.call == digipeater_call).order_by(
            desc(Digipeater.lastheard)).first()

        last_check = session.query(Digipeater.lastcheck).\
            filter(Digipeater.call == digipeater_call).\
            order_by(desc(Digipeater.lastcheck)).first()

        if last_seen is not None:
            last_seen = last_seen[0]

        if last_check is not None:
            last_check = last_check[0]

        if last_check is not None:
            timedelta = (now - last_check)
        else:
            timedelta = None

    except IndexError:
        last_seen = None  # New digi
        timedelta = None

    if digipeater_call not in existing_digipeaters_data and \
            digipeater_call not in added_digipeaters:
        digipeater_info = get_info(digipeater_call, info_method)

        if digipeater_info:
            if verbose:
                print(f"Adding digipeater {digipeater_call}")
            lat = float(digipeater_info[0])
            lon = float(digipeater_info[1])
            grid = digipeater_info[2]

            new_digipeater = Digipeater(
                call=digipeater_call,
                lastheard=timestamp,
                grid=grid,
                geom=f'SRID=4326;POINT({lon} {lat})',
                heard=heard,
                ssid=ssid,
                lastcheck=now
            )

            session.add(new_digipeater)
            digipeater_counter += 1
            added_digipeaters.append(digipeater_call)

    elif timedelta is None or timedelta.days >= refresh_days:
        digipeater_info = get_info(digipeater_call, info_method)

        if digipeater_info:
            if verbose:
                print(f"Updating digipeater {digipeater_call}")
            lat = float(digipeater_info[0])
            lon = float(digipeater_info[1])
            grid = digipeater_info[2]

            session.query(Digipeater). \
                filter(Digipeater.call == digipeater_call). \
                update({Digipeater.geom: f'SRID=4326;POINT({lon} {lat})',
                        Digipeater.lastcheck: now},
                       synchronize_session="fetch")

    # Update timestamp
    if last_seen and last_seen < timestamp:
        session.query(Digipeater).filter(
            Digipeater.call == digipeater_call).update(
            {Digipeater.lastheard: timestamp, Digipeater.heard: heard},
            synchronize_session="fetch")

session.commit()
session.close()

print(f"Added {mh_counter} MH items, {new_op_counter} new ops,"
      f" and {digipeater_counter} digipeaters")
