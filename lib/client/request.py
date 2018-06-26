import socket
import time
import warnings

def copy(items, sites, n = 1):
    if len(items) == 0:
        raise RuntimeError('Missing --dataset or --block.')
    if sites is None:
        raise RuntimeError('Missing --site')

    data = []
    for item in items:
        data.append(('item[]', item))
    for site in sites:
        data.append(('site[]', site))
    data.append(('n', n))

    return data

def pollcopy(rid = None, items = None, sites = None, statuses = None, users = None):
    data = []
    if rid is not None and rid != 0:
        data.append(('request_id', rid))
    if items is not None:
        data.extend(('item[]', item) for item in items)
    if sites is not None:
        data.extend(('site[]', site) for site in sites)
    if statuses is not None:
        data.extend(('status[]', status) for status in statuses)
    if users is not None:
        data.extend(('user[]', user) for user in users)

    if len(data) == 0:
        raise RuntimeError('No selection specified')

    return data

def cancelcopy(rid):
    return {'request_id': rid}

def delete(items, sites):
    if len(items) == 0:
        raise RuntimeError('Missing --dataset or --block.')
    if sites is None:
        raise RuntimeError('Missing --site')

    data = []
    for item in items:
        data.append(('item[]', item))
    for site in sites:
        data.append(('site[]', site))

    return data

def polldelete(rid = None, items = None, sites = None, statuses = None, users = None):
    data = []
    if rid is not None and rid != 0:
        data.append(('request_id', rid))
    if items is not None:
        data.extend(('item[]', item) for item in items)
    if sites is not None:
        data.extend(('site[]', site) for site in sites)
    if statuses is not None:
        data.extend(('status[]', status) for status in statuses)
    if users is not None:
        data.extend(('user[]', user) for user in users)

    if len(data) == 0:
        raise RuntimeError('No selection specified')

    return data
