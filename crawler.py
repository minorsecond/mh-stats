#!/bin/python3

from telnetlib import Telnet
import re
import psycopg2
import datetime
import configparser
from common import get_info
import time
from shapely.geometry import Point

now = datetime.datetime.now().replace(microsecond=0)
refresh_days = 7

config = configparser.ConfigParser()
config.read("settings.cfg")

telnet_user = config['telnet']['username']
telnet_pw = config['telnet']['password']
telnet_ip = config['telnet']['ip']
telnet_port = config['telnet']['port']
pg_user = config['postgres']['username']
pg_pw = config['postgres']['password']
pg_host = config['postgres']['host']
pg_db = config['postgres']['db']
pg_port = config['postgres']['port']

# Connect to PG
con = psycopg2.connect(database=pg_db, user=pg_user,
                       password=pg_pw, host=pg_host, port=pg_port)

now = datetime.datetime.now().replace(microsecond=0)
year = datetime.date.today().year

read_mh = con.cursor()
read_mh.execute("SELECT call FROM packet_mh.mh_list")
existing_ops = read_mh.fetchall()

first_order_ops = []
for existing_op in existing_ops:
    first_order_ops.append(existing_op[0].strip())

# Connect to local telnet server
tn = Telnet(telnet_ip, telnet_port, timeout=5)
tn.read_until(b"user: ", timeout=2)
tn.write(telnet_user.encode('ascii') + b"\r")
tn.read_until(b"password:", timeout=2)
tn.write(telnet_pw.encode('ascii') + b"\r")
tn.read_until(b"Connected", timeout=2)
tn.write("n".encode('ascii') + b"\r")
tn.write(b"\r")
tn.write(b"bye\r")

print(f"Connected to {telnet_ip}")

calls = []
output = tn.read_all()
output = output.split(b"Nodes")[1].strip()
output = output.split(b'***')[0]
output = re.sub(b' +', b' ', output)
output = output.split(b'\r\n')

for row in output:
    calls.extend(row.split(b' '))

cleaned_calls = []
for call in calls:
    if b':' in call:
        call = call.split(b':')[1]
    cleaned_calls.append(call)

while(b"" in cleaned_calls):
    cleaned_calls.remove(b"")

full_info = {}
processed_calls = []
for call in cleaned_calls:
    call = call.decode('utf-8')
    base_call = re.sub(r'[^\w]', ' ', call.split('-')[0])
    if base_call not in processed_calls:
        print(f"Getting info for {base_call}")

        if '-' in call:
            ssid = re.sub(r'[^\w]', ' ', call.split('-')[1])
        else:
            ssid = None

        coords = get_info(base_call)

        full_info[base_call] = (base_call, now, coords, ssid)
        processed_calls.append(base_call)

print(full_info)

