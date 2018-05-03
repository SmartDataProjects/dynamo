import logging

from dynamo.source.userinfo import UserInfoSource
from dynamo.utils.interface.sitedb import SiteDB

LOG = logging.getLogger(__name__)

class SiteDBUserInfoSource(UserInfoSource):
    def __init__(self, config):
        UserInfoSource.__init__(self, config)
        
        self._sitedb = SiteDB(config.get('sitedb', None))

    def get_user(self, name): #override
        result = self._sitedb.make_request('people', ['match=%s' % name])

        if len(result) == 0:
            return None
        else:
            user_info = result[0]
            name = user_info[0]
            email = user_info[1]
            dn = user_info[4]

            return (name, email, dn)

    def get_user_list(self): #override
        result = self._sitedb.make_request('people')

        all_users = {}
        for user in users:
            user_info = result[0]
            name = user_info[0]
            email = user_info[1]
            dn = user_info[4]
            
            all_users[name] = (name, email, dn)

        return all_users
