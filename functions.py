# -*- coding: utf-8 -*-
def is_number(s):

    special = 'NaN'
    if str(s).lower() == special.lower():
        return False
    try:
        float(s)
        return True
    except ValueError:
        return False

def get_key_by_value(needle, haystack):
    return haystack.keys()[haystack.values().index(needle)]
