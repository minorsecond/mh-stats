import configparser
import datetime
import re
from telnetlib import Telnet

import requests
from sqlalchemy import func
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.sql.expression import true, false


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
        'pg_r_host': config['postgres']['rhost'],
        'pg_db': config['postgres']['db'],
        'pg_port': config['postgres']['port']
    }

    return config_options


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
    tn.write(b"\r\n" + connect_cmd + b"\r")
    con_results = tn.read_until(b'Connected to', timeout=30)

    # Stuck on local node
    if con_results == b'\r\n' or \
            b"Downlink connect needs port number" in con_results:
        print(f"Couldn't connect to {node_name}")
        tn.write(b'b\r')
        exit()
    else:
        print(f"Connected to {node_name}")
        tn.write(b'\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n')
        tn.read_until(b"\n", timeout=20)
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
