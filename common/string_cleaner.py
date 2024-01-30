import re


def strip_call(node_call):
    """
    Strips node call into components
    :param node_call: A node name string like kd5lpb-7
    :return: CALL-SSID, CALL, SSID
    """

    # Call includes ssid, ie KD5LPB-7
    call = node_call.strip().upper()
    # Op_call is just the call, ie KD5LPB
    op_call = re.sub(r'[^\w]', ' ', call.split('-')[0].strip())

    # Get SSID of call if it exists
    if '-' in call:
        ssid = int(re.sub(r'[^\w]', ' ', call.split('-')[1]))
    else:
        ssid = None

    return call, op_call, ssid


def remove_dupes(call_list):
    "Returns list with one alias:call pair per node"
    a_list = []
    b_list = []
    res = []

    for element in call_list:
        if len(element) > 1:
            a = element[0]
            b = element[1]

            if isinstance(a, bytes):
                a_temp = a.decode('utf-8')
                b_temp = b.decode('utf-8')
            else:
                a_temp = a
                b_temp = b

            # Get callsign base
            a_base = re.sub(r'[^\w]', ' ', a_temp.split('-')[0])
            b_base = re.sub(r'[^\w]', ' ', b_temp.split('-')[0])

            if a_base not in a_list and a_base not in b_list and b_base \
                    not in a_list and b_base not in b_list:
                res.append([a, b])
                a_list.append(a_base)
                b_list.append(b_base)
        else:
            res.append(element)

    return res


def clean_calls(calls_to_clean):
    """
    Cleans output from telnet, removing : and whitespaces
    :param calls_to_clean: List of calls from Telnet. Each item looks like
    b'LPBNOD:KD5LPB-7'
    :return: A list of lists. Each list element looks like
    ['LPBNOD', 'KD5LPB-7]
    """

    cleaned_calls = []
    for call in calls_to_clean:
        if ':' in call:
            call = call.split(':')
            while (b'' in call):
                call.remove(b'')
            cleaned_calls.append(call)

        else:
            cleaned_calls.append([None, call])

    cleaned_calls = [[string for string in sublist if string] for sublist in
                     cleaned_calls]
    cleaned_calls = [e for e in cleaned_calls if e != []]
    cleaned_calls = remove_dupes(cleaned_calls)

    return cleaned_calls
