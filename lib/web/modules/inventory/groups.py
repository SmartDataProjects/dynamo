import fnmatch
import re

from dynamo.web.modules._base import WebModule

class ListGroups(WebModule):
    def run(self, caller, request, inventory):
        # collect information from the inventory and registry according to the requests

        response = []

        if 'group' in request:
            match_name = request.getvalue('group')
            if '*' in match_name:
                pattern = re.compile(fnmatch.translate(match_name))
                for name in sorted(inventory.groups.iterkeys()):
                    if pattern.match(name):
                        response.append({'name': name})
                
            elif match_name in inventory.groups:
                response.append({'name': match_name})

        else:
            for name in sorted(inventory.groups.iterkeys()):
                if name is None:
                    response.append({'name': '(no group)'})
                else:
                    response.append({'name': name})
    
        # return any JSONizable python object (maybe should be limited to a list)
        return response

# exported to __init__.py
export_data = {'groups': ListGroups}
