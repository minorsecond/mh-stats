import configparser
import re

import requests


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