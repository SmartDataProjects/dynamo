class WebModule(object):
    def __init__(self, config):
        self.write_enabled = False

    def run(self, caller, request, inventory):
        """
        Main module code.
        @param caller    WebServer.User object (namedtuple of (name, id, authlist))
        @param request   A dictionary (or list if JSON list is uploaded) of user request
        @param inventory The inventory
        """

        raise NotImplementedError('run')
