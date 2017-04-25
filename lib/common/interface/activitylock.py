import time
import logging

from common.interface.webservice import RESTService
import common.configuration as config

logger = logging.getLogger(__name__)

class ActivityLock(object):
    """
    Web-based activity lock using registry.
    """

    def __init__(self):
        self._rest = RESTService(config.activitylock.url_base)

    def lock(self, application, service = 'dynamo', asuser = ''):
        while True:
            options = ['app=' + application]
            if service:
                options.append('service=' + service)
            if asuser:
                options.append('asuser=' + asuser)

            response = self._rest.make_request('lock', options)
            if response['message'].startswith('OK'):
                break
                
            logger.info('Activity lock: %s', str(response))
            time.sleep(60)

    def unlock(self, application, service = 'dynamo', asuser = ''):
        options = ['app=' + application]
        if service:
            options.append('service=' + service)
        if asuser:
            options.append('asuser=' + asuser)

        response = self._rest.make_request('unlock', options)

        return response['message'].startswith('OK')
