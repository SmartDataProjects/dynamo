class UserDataMixin(object):
    """
    Mixin to use the authorizer object to manage user data.
    """

    def __init__(self, config):
        self.require_authorizer = True
        self.authorizer = None
