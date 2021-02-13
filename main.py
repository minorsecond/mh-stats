# Save MH data to PostgreSQL database

from telnetlib import Telnet
import re
import psycopg2
import datetime
import configparser
import requests
from shapely.geometry import Point

refresh_days = 1

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

    lat = None
    lon = None
    grid = None

    if callsign:
        callsign = re.sub(r'[^\w]', ' ', callsign)

        req = f"http://api.hamdb.org/{callsign}/json/mh-stats"
        http_results = requests.get(req).json()

        lat = http_results['hamdb']['callsign']['lat']
        lon = http_results['hamdb']['callsign']['lon']
        grid = http_results['hamdb']['callsign']['grid']

    else:
        return None

    if lat == "NOT_FOUND" or lon == "NOT_FOUND" or grid == "NOT_FOUND":
        return None

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
read_digipeaters.execute("SELECT call, ST_X(geom), ST_Y(geom) FROM packet_mh.digipeaters")
existing_digipeaters = read_digipeaters.fetchall()

existing_digipeaters_data = {}
for digipeater in existing_digipeaters:
    call = digipeater[0]
    lon = digipeater[1]
    lat = digipeater[2]
    existing_digipeaters_data[call] = (lat, lon)

# Write to PG
digipeater_list = {}
current_op_list = []
write_cursor = con.cursor()
for item in radio_mh_list:
    call = item[0]
    op_call = call.split('-')[0]
    timestamp = item[1]
    timedelta = now - timestamp
    hms = timestamp.strftime("%H:%M:%S")
    lat = None
    lon = None
    point = None
    grid = None

    digipeaters = ""
    try:
        for digipeater in item[2]:
            digipeaters += f"{digipeater},"
            digipeater_list[digipeater] = timestamp
    except TypeError:
        digipeaters = None

    # Write MH table
    if f"{call} {hms}" not in existing_mh_data:
        print(f"{now} Adding {call} at {timestamp} through {digipeaters}.")
        write_cursor.execute(f"INSERT INTO packet_mh.mh_list (timestamp,call,digipeaters,op_call) VALUES ('{timestamp}','{call}','{digipeaters}', '{op_call}')")

    # Update ops last heard
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
        write_cursor.execute(f"INSERT INTO packet_mh.operators (call, lastheard, geom) VALUES ('{op_call}', '{timestamp}', st_setsrid('{point}'::geometry, 4326))")
        current_op_list.append(op_call)

    elif timedelta.days >= refresh_days and op_call not in current_op_list:
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
for digipeater in digipeater_list.items():
    lat = None
    lon = None
    digipeater_call = digipeater[0]
    timestamp = digipeater[1]
    timedelta = (now - timestamp)
    heard = False

    if '*' in digipeater_call:
        heard = True

    digipeater_call = re.sub(r'[^\w]', ' ', digipeater_call)

    if digipeater_call not in existing_digipeaters_data:
        digipeater_info = get_info(digipeater_call.split('-')[0])

        if digipeater_info:
            print(f"Adding digipeater {digipeater_call}")
            lat = float(digipeater_info[0])
            lon = float(digipeater_info[1])
            point = Point(lon, lat).wkb_hex
            grid = digipeater_info[2]

            write_cursor.execute(f"INSERT INTO packet_mh.digipeaters (call, lastheard, grid, geom, heard) VALUES ('{digipeater_call}', '{timestamp}', '{grid}', st_setsrid('{point}'::geometry, 4326), '{heard}')")

    elif timedelta.days >= refresh_days:
        if (lat, lon, grid) != existing_digipeaters_data.get(digipeater_call):
            print(f"Updating digipeater coordinates for {digipeater}")
            update_digi_query = f"UPDATE packet_mh.operators SET geom = st_setsrid('{point}'::geometry, 4326) WHERE call = '{digipeater_call}';"
            write_cursor.execute(update_digi_query)

    # Update timestamp
    update_digi_query = f"UPDATE packet_mh.digipeaters SET lastheard = '{timestamp}' WHERE call = '{digipeater_call}';"
    write_cursor.execute(update_digi_query)

con.commit()
con.close()
