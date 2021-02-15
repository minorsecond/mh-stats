import re

import requests


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