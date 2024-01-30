import configparser
import datetime
import re
import sys
from time import sleep
from telnetlib import Telnet

import requests
from sqlalchemy import func
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.sql.expression import true, false

import qrz


def get_conf():
    """
    Get config data
    :return: Dict of config options
    """

    config = configparser.ConfigParser()
    config.read("settings.cfg")

    config_options = {
        'telnet_user': config['telnet']['username'],
        'telnet_pw': config['telnet']['password'],
        'telnet_ip': config['telnet']['ip'],
        'telnet_port': config['telnet']['port'],
        'pg_user': config['postgres']['username'],
        'pg_pw': config['postgres']['password'],
        'pg_host': config['postgres']['host'],
        'remote_pg_host': config['postgres']['remote_pg_host'],
        'pg_db': config['postgres']['db'],
        'pg_port': config['postgres']['port'],
        'info_method': config['mhcrawler']['info_method']
    }

    return config_options


def get_info(callsign, method):
    """
    Get info from hamdb
    :param callsign: Callsign string
    :return: call & lat/long
    """

    lat = None
    lon = None
    grid = None
    max_retries = 3
    retry_delay = 5

    if callsign:
        callsign = re.sub(r'[^\w]', ' ', callsign)
        match = re.match(r'[A-Za-z0-9]*([a-zA-Z]+[0-9]+|[0-9]+[a-zA-Z]+)',
                            callsign)

        if match is None:
            return None

        if method == "hamdb":
            req = f"http://api.hamdb.org/{callsign}/json/mh-stats"
            http_results = None
            for attempt in range(max_retries):
                try:
                    http_results = requests.get(req).json()
                    break  # If the request is successful, exit the loop
                except Exception as e:
                    print(f"Attempt {attempt + 1} failed: {e}")
                    if attempt < max_retries - 1:
                        print(f"Retrying in {retry_delay} seconds...")
                        sleep(retry_delay)
                    else:
                        return None

            lat = http_results['hamdb']['callsign']['lat']
            lon = http_results['hamdb']['callsign']['lon']
            grid = http_results['hamdb']['callsign']['grid']

            if lat == "NOT_FOUND" or lon == "NOT_FOUND" or grid == "NOT_FOUND":
                return None
        else:
            try:
                qrz_instance = qrz.QRZ(cfg="settings.cfg")
                http_results = qrz_instance.callsign(callsign)
                lat = http_results['lat']
                lon = http_results['lon']
                grid = http_results['grid']
            except qrz.CallsignNotFound or qrz.QRZsessionNotFound:
                return None

    else:
        return None

    return (lat, lon, grid)


def telnet_connect():
    """
    Connect to telnet & return telnet object
    :return: Telnet object
    """
    conf = get_conf()
    tn = Telnet(conf['telnet_ip'], conf['telnet_port'], timeout=5)
    tn.read_until(b"user: ", timeout=2)
    tn.write(conf['telnet_user'].encode('ascii') + b"\r")
    tn.read_until(b"password:", timeout=2)
    tn.write(conf['telnet_pw'].encode('ascii') + b"\r")
    tn.read_until(b'Telnet Server\r\n', timeout=20)

    return tn


def node_connect(node_name, tn):
    """
    Connect to node
    :param node_name: Name of node to connect to
    :param tn: A Telnet connection object
    :return: Telnet object
    """

    connect_cmd = f"c {node_name}".encode('ascii')
    print(f"Connecting to {node_name}")

    try:
        tn.write(b"\r\n" + connect_cmd + b"\r")
        con_results = tn.read_until(b'Connected to', timeout=30)
    except ConnectionResetError:
        print("Connection reset")
        return None

    # Stuck on local node
    if con_results == b'\r\n' or \
            b"Downlink connect needs port number" in con_results:
        print(f"Couldn't connect to {node_name}")
        tn.write(b'b\r')
        exit()
    else:
        print(f"Connected to {node_name}")
        #tn.write(b'\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n')
        #tn.read_until(b"\n", timeout=20)
        sleep(3)
        return tn


def auto_node_selector(CrawledNode, session, refresh_days):
    """
    Automatically get a node to crawl
    :param CrawledNode: a DB Object
    :param session: A session object
    :param refresh_days: Days ago to select from DB
    :return: A dict containing (id, port, last_crawled, port_name)
    """
    node_to_crawl_info = None
    refresh_time = datetime.datetime.utcnow().replace(microsecond=0) - \
                   datetime.timedelta(days=refresh_days)
    # Get a node that hasn't been crawled in 2 weeks

    try:
        # Get a node port that doesn't need check and is active
        crawled_nodes = session.query(CrawledNode).filter(
            CrawledNode.last_crawled < refresh_time). \
            filter(CrawledNode.needs_check == false(),
                   CrawledNode.active_port == true()). \
            order_by(func.random()).limit(1).one_or_none()
        if crawled_nodes:
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

    if node_to_crawl_info is None:
        print("Nothing to crawl")
        exit()

    return node_to_crawl_info
