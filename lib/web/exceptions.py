class MissingParameter(Exception):
    """Raise if request is missing a required parameter."""
    def __init__(self, param_name, context = None):
        self.param_name = param_name
        self.context = context

class IllFormedRequest(Exception):
    """Raise if a request parameter value does not conform to a format."""
    def __init__(self, param_name, value, hint = None, allowed = None):
        self.param_name = param_name
        self.value = value
        self.hint = hint
        self.allowed = allowed

class InvalidRequest(Exception):
    """Raise if the request cannot be fulfilled."""
    pass

class AuthorizationError(Exception):
    """Raise if the user is not authorized for the request."""
    pass

class ResponseDenied(Exception):
    """Raise when there is nothing technically wrong but response is denied (e.g. return string too long)"""
    pass
