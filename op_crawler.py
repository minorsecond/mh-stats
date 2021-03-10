import argparse
import datetime
import re
from string import digits
from time import sleep

from sqlalchemy import func
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.exc import NoResultFound

from common import get_conf, telnet_connect, node_connect
from models.db import engine, ops_to_crawl

debug = True

parser = argparse.ArgumentParser(description="Crawl packet operators")
parser.add_argument('--operator', metavar='N', type=str,
                    help="Op call to crawl")
parser.add_argument('-auto', action='store_true',
                    help="Pick an operator to crawl automatically")
args = parser.parse_args()
op_to_crawl = args.operator
auto = args.auto

if op_to_crawl:
    op_to_crawl = op_to_crawl.strip().upper()

if auto and op_to_crawl:
    print("You can't enter op to crawl & auto mode")
    exit()
elif not op_to_crawl and not debug:
    print("You must enter a node to crawl.")
    exit()
elif not op_to_crawl and not auto:
    print("You must select an option")
    exit()

conf = get_conf()
year = datetime.date.today().year
Session = sessionmaker(bind=engine)
session = Session()

refresh_time = datetime.datetime.utcnow().replace(microsecond=0) - \
               datetime.timedelta(days=7)

"""
Get an op to crawl. The tuple result is in the form:
parent_call, remote_call, last_crawled, port, ssids (list)
"""
conn_info = None

if auto:
    try:
        op_to_crawl = session.query(ops_to_crawl). \
            filter(ops_to_crawl.c.last_crawled < refresh_time). \
            order_by(func.random()).limit(1).one_or_none()

        conn_info = (op_to_crawl.parent_call,
                     op_to_crawl.remote_call,
                     op_to_crawl.last_crawled,
                     op_to_crawl.port,
                     op_to_crawl.port_name,
                     op_to_crawl.ssids)
        print(conn_info)

    except NoResultFound:
        print("Nothing to crawl")
        exit()
else:
    if debug:
        conn_info = ('KD5LPB-7', None, None, 1, "145.050 MHz 1200 Baud", 7)

# Connect to local telnet server and then to next node
if conn_info:
    tn = telnet_connect()
    tn = node_connect(conn_info[0], tn)

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

        # Check if port name has changed
        selected_port = conn_info[3]
        selected_port_name = conn_info[4]
        port_name = available_ports[selected_port - 1].decode(
            'utf-8').strip().lstrip(digits).strip()

        if selected_port_name != port_name:  # Port name has changed
            print(f"Port name has changed from {selected_port_name} to "
                  f"{port_name}")
            tn.write(b'b\r')
            exit()

    except IndexError:
        # Corrupt data
        print(f"Possible corrupt data received: {available_ports_raw}")
        exit()

# Let connection settle, and then connect to operator station
sleep(3)

if not debug:
    op_to_call = conn_info[1]
    con_string = f"c {selected_port} {op_to_call}".encode('ascii')
    tn.write(con_string)
    sleep(3)

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
