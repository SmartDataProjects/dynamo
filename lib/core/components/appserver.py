import os
import random
import threading
import Queue

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

        ## Queues synchronous applications will wait on. {app_id: Queue}
        self.synch_app_queues = {}
        ## notify_synch_lock can be called from the DynamoServer immediately
        ## after the application is scheduled. Need a lock to make sure we
        ## register them first.
        self.notify_lock = threading.Lock()

    def start(self):
        """Start the server."""

        raise NotImplementedError('start')

    def stop(self):
        """Stop the server."""

        raise NotImplementedError('stop')

    def notify_synch_app(self, app_id, data):
        """
        Notify synchronous app.
        @param app_id  App id (key in synch_app_queues)
        @param data    Dictionary passed to thread waiting to start a synchronous app.
        """
        with self.notify_lock:
            try:
                self.synch_app_queues[app_id].put(data)
            except KeyError:
                pass

    def wait_synch_app_queue(self, app_id):
        """
        Wait on queue and return the data put in the queue.
        @param app_id  App id (key in synch_app_queues)
        """
        return self.synch_app_queues[app_id].get()

    def _make_workarea(self):
        """
        Make a work area under spool with a random 64-bit hex as the name. This can be a static function.
        """

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

    def _schedule_app(self, mode, **app_data):
        """
        Call schedule_application on the master server. If mode == 'synch', create a communication
        queue and register it under synch_app_queues. The server should then wait on this queue
        before starting the application.
        """
        with self.notify_lock:
            app_id = self.dynamo_server.manager.master.schedule_application(**app_data)
            if mode == 'synch':
                self.synch_app_queues[app_id] = Queue.Queue()

        return app_id
