import dynamo.web.exceptions as exceptions

_yes = ['true', '1', 'y', 'yes']
_no = ['false', '0', 'n', 'no']
def yesno(request, name, default = True):
    try:
        req = request[name]
    except KeyError:
        return default

    if req is None:
        return default

    req = req.lower()
    if req in _yes:
        return True
    elif req in _no:
        return False
    else:
        raise exceptions.IllFormedRequest(name, request[name], allowed = _yes + _no)
