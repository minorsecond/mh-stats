# Save MH data to PostgreSQL database

from telnetlib import Telnet
import re
import psycopg2
import datetime
import configparser
import requests

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


def get_info(callsign):
    """
    Get info from hamdb
    :param callsign: Callsign string
    :return: call & lat/long
    """

    callsign = re.sub(r'[^\w]', ' ', callsign)
    print(f"Getting info for {callsign}")

    req = f"http://api.hamdb.org/{callsign}/json/mh-stats"
    http_results = requests.get(req).json()

    lat = http_results['hamdb']['callsign']['lat']
    lon = http_results['hamdb']['callsign']['lon']
    grid = http_results['hamdb']['callsign']['grid']

    return (lat, lon, grid)


# Connect to PG
con = psycopg2.connect(database=pg_db, user=pg_user,
                       password=pg_pw, host=pg_host, port=pg_port)

now = datetime.datetime.now().replace(microsecond=0)
year = datetime.date.today().year

tn = Telnet(telnet_ip, telnet_port, timeout=5)
tn.read_until(b"user: ", timeout=2)
tn.write(telnet_user.encode('ascii') + b"\r")
tn.read_until(b"password:", timeout=2)
tn.write(telnet_pw.encode('ascii') + b"\r")
tn.read_until(b"Connected", timeout=2)
tn.write("mhu 1".encode('ascii') + b"\r")
tn.write(b"\r")
tn.write(b"bye\r")

print(f"Connected to {telnet_ip}")

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

results = []
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

    results.append(res)

"""
# Turn into dict
received_dict = {}

for index_1, data in enumerate(output):
    print(data)
    call = data[0]
    timestamp = data[1]
    digipeaters = []

    #print(data)
    # try digipeaters
    for index_2, entry in enumerate(data):
        if index_2 > 2:
            print(index_2)
            digipeaters.append(entry[index_2])

    received_dict[call] = (timestamp, digipeaters)

"""

# Write to PG, first get existing data to check for duplicates
cur = con.cursor()
cur.execute("SELECT * FROM packet_mh.mh_list")
existing_rows = cur.fetchall()

existing_data = []
for row in existing_rows:
    call = row[2]
    timestamp = row[1]
    hms = timestamp.strftime("%H:%M:%S")
    existing_data.append(f"{call} {hms}")

results = sorted(results, key=lambda x: x[1], reverse=False)

cur = con.cursor()
for item in results:
    call = item[0]
    timestamp = item[1]
    hms = timestamp.strftime("%H:%M:%S")
    lat = None
    lon = None
    grid = None

    digipeaters = ""
    try:
        for digipeater in item[2]:
            digipeaters += f"{digipeater},"
    except TypeError:
        digipeaters = None

    if f"{call} {hms}" not in existing_data:
        # add coordinates & grid
        info = get_info(call.split('-')[0])
        lat = info[0]
        lon = info[1]
        grid = info[2]

        print(f"{now} Adding {call} at {timestamp} through {digipeaters}.")
        cur.execute(f"INSERT INTO packet_mh.mh_list (timestamp,call,digipeaters, lat, lon, grid) VALUES ('{timestamp}','{call}','{digipeaters}','{lat}','{lon}','{grid}')")
con.commit()
con.close()
