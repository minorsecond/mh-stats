#!/bin/python3
# Save MH data to PostgreSQL database

import datetime
import re
from telnetlib import Telnet

from sqlalchemy import func, desc
from sqlalchemy.orm import sessionmaker

from common import get_info, get_conf
from models.db import engine, LocallyHeardStation, Operator, \
    Digipeater

refresh_days = 7

conf = get_conf()

# Connect to PG
Session = sessionmaker(bind=engine)
session = Session()

# Connect to PG

now = datetime.datetime.utcnow().replace(microsecond=0)
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
                             func.st_y(Operator.geom)).all()

existing_ops_data = {}
for op in existing_ops:
    call = op[1]
    lon = op[2]
    lat = op[3]
    existing_ops_data[call] = (lat, lon)

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
for item in radio_mh_list:
    call = item[0].strip()
    op_call = re.sub(r'[^\w]', ' ', call.split('-')[0].strip())

    try:
        ssid = re.sub(r'[^\w]', ' ', call.split('-')[1].strip())
        ssid = int(ssid)
    except IndexError or TypeError:
        ssid = None

    timestamp = item[1]

    try:
        last_heard = session.query(Operator.lastheard). \
            filter(Operator.call == op_call). \
            order_by(desc(Operator.lastheard)).first()
        if last_heard:
            last_heard = last_heard[0]
            timedelta = (now - last_heard)
        else:
            last_heard = None
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
        print(f"{now} Adding {call} at {timestamp} through {digipeaters}.")

        new_mh_entry = LocallyHeardStation(
            timestamp=timestamp,
            call=call,
            digipeaters=digipeaters,
            op_call=op_call,
            ssid=ssid
        )
        session.add(new_mh_entry)

    # Update ops last heard
    if last_heard and timestamp > last_heard:
        session.query(Operator).filter(Operator.call == op_call).update(
            {Operator.lastheard: timestamp}, synchronize_session="fetch")

    # Write Ops table if
    if op_call not in existing_ops_data and op_call not in current_op_list:
        # add coordinates & grid
        info = get_info(call.split('-')[0])

        if info:
            lat = float(info[0])
            lon = float(info[1])
            grid = info[2]

            print(f"{now} Adding {op_call} to operator table.")
            new_operator = Operator(
                call=op_call,
                lastheard=timestamp,
                geom=f'SRID=4326;POINT({lon} {lat})',
                grid=grid
            )
            session.add(new_operator)
            current_op_list.append(op_call)

    elif timedelta and timedelta.days >= refresh_days and op_call not in \
            current_op_list:
        # add coordinates & grid

        info = get_info(call.split('-')[0])

        if info:
            lat = float(info[0])
            lon = float(info[1])
            grid = info[2]

        if (lat, lon, grid) != existing_ops_data.get(call):
            print(f"Updating coordinates for {op_call}")
            session.query(Operator).filter(Operator.call == op_call).update(
                {Operator.geom: f'SRID=4326;POINT({lon} {lat})',
                 Operator.lastheard: last_heard},
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
        if last_seen:
            last_seen = last_seen[0]
            timedelta = (now - last_seen)
        else:
            last_seen = None
            timedelta = None
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

            new_digipeater = Digipeater(
                call=digipeater_call,
                lastheard=timestamp,
                grid=grid,
                geom=f'SRID=4326;POINT({lon} {lat})',
                heard=heard,
                ssid=ssid
            )

            session.add(new_digipeater)

            added_digipeaters.append(digipeater_call)

    elif timedelta and timedelta.days >= refresh_days:
        digipeater_info = get_info(digipeater_call)

        if digipeater_info:
            print(f"Adding digipeater {digipeater_call}")
            lat = float(digipeater_info[0])
            lon = float(digipeater_info[1])
            grid = digipeater_info[2]
            print(f"Updating digipeater coordinates for {digipeater}")

            session.query(Digipeater). \
                filter(Digipeater.call == digipeater_call). \
                update({Digipeater.geom: f'SRID=4326;POINT({lon} {lat})'},
                       synchronize_session="fetch")

    # Update timestamp
    if last_seen and last_seen < timestamp:
        session.query(Digipeater).filter(
            Digipeater.call == digipeater_call).update(
            {Digipeater.lastheard: timestamp, Digipeater.heard: heard},
            synchronize_session="fetch")

session.commit()
session.close()
