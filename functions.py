# -*- coding: utf-8 -*-

def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False

def get_key_by_value(needle, haystack):
    return haystack.keys()[haystack.values().index(needle)]
