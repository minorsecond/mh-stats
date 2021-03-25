from common import string_cleaner
import pytest


def test_strip_call():
    call, op_call, ssid = string_cleaner.strip_call('kd5lpb-7')
    assert call == 'KD5LPB-7'
    assert op_call == 'KD5LPB'
    assert ssid == 7


def test_remove_dupes():
    dupe_list = [['LPBNOD', 'KD5LPB-7'], ['KD5LPB-7', 'LPBNOD'],
                 ['COSCO', 'KE0GB-7'],['KE0GB-7', 'COSCO']]
    deduped_list = string_cleaner.remove_dupes(dupe_list)
    assert deduped_list == [['LPBNOD', 'KD5LPB-7'], ['COSCO', 'KE0GB-7']]
