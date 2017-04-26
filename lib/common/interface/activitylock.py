import time
import logging

from common.interface.webservice import RESTService
import common.configuration as config

logger = logging.getLogger(__name__)

class ActivityLock(object):
    """
    Web-based activity lock using registry.
    """

    def __init__(self, application, service = 'dynamo', asuser = ''):
        self._rest = RESTService(config.activitylock.url_base)
        self.application = application
        self.service = service
        self.asuser = asuser

    def __enter__(self):
        self.lock()

    def __exit__(self, exc_type, exc_value, traceback):
        if not self.unlock():
            raise RuntimeError('Failed to unlock')

        return exc_type is None and exc_value is None and traceback is None

    def lock(self):
        while True:
            options = ['app=' + self.application]
            if self.service:
                options.append('service=' + self.service)
            if self.asuser:
                options.append('asuser=' + self.asuser)

            response = self._rest.make_request('lock', options)
            if response['message'].startswith('OK'):
                break
                
            logger.info('Activity lock: %s', str(response))
            time.sleep(60)

    def unlock(self):
        options = ['app=' + self.application]
        if self.service:
            options.append('service=' + self.service)
        if self.asuser:
            options.append('asuser=' + self.asuser)

        response = self._rest.make_request('unlock', options)

        return response['message'].startswith('OK')
