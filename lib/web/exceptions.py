class MissingParameter(Exception):
    """Raise if request is missing a required parameter."""
    def __init__(self, param_name, context = None):
        self.param_name = param_name
        self.context = context

    def __str__(self):
        msg = 'Missing required parameter "%s"' % self.param_name
        if self.context is not None:
            msg += ' in %s' % self.context
        msg += '.\n'

        return msg

class ExtraParameter(Exception):
    """Raise if there is an excess parameter."""
    def __init__(self, param_name, context = None):
        """
        @param param_name  A string of a list
        @param context
        """
        self.param_name = param_name
        self.context = context

    def __str__(self):
        if type(self.param_name) is list:
            msg = 'Parameters %s not expected' % str(self.param_name)
        else:
            msg = 'Parameter "%s" not expected' % str(self.param_name)
        if self.context is not None:
            msg += ' in %s' % self.context
        msg += '.\n'

        return msg

class IllFormedRequest(Exception):
    """Raise if a request parameter value does not conform to a format."""
    def __init__(self, param_name, value, hint = None, allowed = None):
        self.param_name = param_name
        self.value = value
        self.hint = hint
        self.allowed = allowed

    def __str__(self):
        msg = 'Parameter "%s" has illegal value "%s".' % (self.param_name, self.value)
        if self.hint is not None:
            msg += ' ' + self.hint + '.'
        if self.allowed is not None:
            msg += ' Allowed values: [%s]' % ['"%s"' % v for v in self.allowed]
        msg += '\n'

        return msg

class InvalidRequest(Exception):
    """Raise if the request values are invalid."""
    def __str__(self):
        if len(self.args) != 0:
            return self.args[0] + '.\n'
        else:
            return 'InvalidRequest\n'

class AuthorizationError(Exception):
    """Raise if the user is not authorized for the request."""
    def __str__(self):
        return 'User not authorized to perform the request.\n'

class ResponseDenied(Exception):
    """Raise when there is nothing technically wrong but response is denied (e.g. return string too long)"""
    def __str__(self):
        return 'Server denied response due to: %s\n' % str(self)

class TryAgain(Exception):
    """Raise when the request cannot be served temporarily."""
    def __str__(self, message = None):
        if message is None:
            return 'Server temporarily not available. Please try again in a few moments.'
        else:
            return 'Server temporarily not available: ' + message
