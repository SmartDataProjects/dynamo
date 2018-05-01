import os

modules = {}

# Import all .py files and subdirectories in this package
# The name of the module (.py file) or package (subdirectory) becomes SCRIPT_NAME
# The module or the __init__.py (for packages) must define a dictionary named exports which define a mapping
# from PATH_INFO to a class that has a run() function.
# Yes this is gross
_moddir = os.path.dirname(__file__)
for pyfile in os.listdir(_moddir):
    if pyfile.startswith('_') or (not os.path.isdir(_moddir + '/' + pyfile) and not pyfile.endswith('.py')):
        continue

    module = pyfile.replace('.py', '')
    modules[module] = __import__('dynamo.web.modules.' + module, globals(), locals(), ['exports']).exports
