import os
import re
import json

from dynamo.utils.transform import unicode2str

def parse_config(value):
    if type(value) is str:
        matches = re.findall('\$\(([^\)]+)\)', value)
        for match in matches:
            value = value.replace('$(%s)' % match, os.environ[match])

        return value

    elif type(value) is dict or type(value) is Configuration:
        return Configuration(value)

    elif type(value) is list:
        result = []
        for item in value:
            result.append(parse_config(item))

        return result

    else:
        return value


class Configuration(dict):
    """
    Configuration object. Basically a dict, but allows access to elements with getattr.
    Also translates string with pattern $(VARIABLE) to environment variable VARIABLE.
    """

    def __init__(self, _arg = None, **kwd):
        if _arg is None:
            config = dict()
        elif type(_arg) is dict or type(_arg) is Configuration:
            config = dict(_arg)
        elif type(_arg) is file:
            config = json.loads(_arg.read())
            unicode2str(config)
        elif type(_arg) is str:
            with open(_arg) as source:
                config = json.loads(source.read())
                unicode2str(config)

        config.update(kwd)

        for key, value in config.iteritems():
            self[key] = parse_config(value)

    def __getattr__(self, attr):
        return self[attr]

    def __setattr__(self, attr, value):
        self[attr] = value

    def __repr__(self):
        return self.dump_json()

    def get(self, attr, default):
        """Return the default value if attr is not found."""
        try:
            return self[attr]
        except KeyError:
            return default

    def clone(self):
        return Configuration(self)

    def dump_json(self, indent = -1):
        if indent >= 0:
            def dump_with_indent(cont, idt):
                try:
                    keys = cont.keys()
                except AttributeError:
                    return json.dumps(cont)
                else:
                    if len(keys) == 0:
                        return '{}'
                    else:
                        js = '{\n'
        
                        cont_lines = []
                        for key in keys:
                            line = ' ' * (idt + 2)
                            line += '"%s": %s' % (key, dump_with_indent(cont[key], idt + 2))
                            cont_lines.append(line)
        
                        js += ',\n'.join(cont_lines)
                        js += '\n' + (' ' * idt) + '}'
                        return js
    
            return dump_with_indent(self, indent)
    
        else:
            return json.dumps(self)
