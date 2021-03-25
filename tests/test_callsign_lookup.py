import pytest
from common import get_info


def test_bad_callsign_lookup():
    bad_results = get_info('SYDNEY', 'QRZ')
    assert bad_results is None


def test_good_callsign_lookup():
    good_results_1 = get_info('KD5LPB', 'QRZ')
    good_results_2 = get_info('PD2SKZ', 'QRZ')
    good_results_3 = get_info('IK6IHL', 'QRZ')
    good_results_4 = get_info('CT1EBQ', 'QRZ')
    good_results_5 = get_info('US1GHQ', 'QRZ')

    assert good_results_1 == ('39.603100', '-104.699620', 'DM79po')
    assert good_results_2 == ('52.006630', '4.439906', 'JO22fa')
    assert good_results_3 == ('42.020833', '13.958333', 'JN62xa')
    assert good_results_4 == ('39.604167', '-8.125000', 'IM59wo')
    assert good_results_5 == ('46.704056', '32.684583', 'KN66iq')
