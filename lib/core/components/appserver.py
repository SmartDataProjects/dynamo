import os
import random

class AppServer(object):
    """Base class for application server."""

    @staticmethod
    def get_instance(module, dynamo_server, config):
        import dynamo.core.components.impl as impl
        cls = getattr(impl, module)
        if not issubclass(cls, AppServer):
            raise RuntimeError('%s is not a subclass of AppServer' % module)

        return cls(dynamo_server, config)

    def __init__(self, dynamo_server, config):
        self.dynamo_server = dynamo_server

    def start(self):
        """Start the server."""

        raise NotImplementedError('start')

    def stop(self):
        """Stop the server."""

        raise NotImplementedError('stop')

    def notify_synch_app(app_id, status = None, path = None):
        """
        Notify synchronous app.
        @param status  If the app cannot execute for some reason, set to application status.
        @param path    If the app is greenlighted, path of the work area.
        """

        raise NotImplementedError('notify_synch_app')

    def make_workarea(self):
        workarea = os.environ['DYNAMO_SPOOL'] + '/work/'
        while True:
            d = hex(random.randint(0, 0xffffffffffffffff))[2:-1]
            try:
                os.makedirs(workarea + d)
            except OSError:
                if not os.path.exists(workarea + d):
                    return ''
                else:
                    # remarkably, the directory existed
                    continue

            workarea += d
            break

        return workarea
