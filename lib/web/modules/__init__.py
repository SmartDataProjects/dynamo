import os

modules = {'data': {}, 'web': {}, 'registry': {}} # registry for backward compatibility

def load_modules():
    # Import all .py files and subdirectories in this package
    # The name of the module (.py file) or package (subdirectory) becomes SCRIPT_NAME
    # The module or the __init__.py (for packages) must define dictionaries named export_data and export_web which define a mapping
    # from PATH_INFO to a class that has a run() function.
    # Yes this is gross
    _moddir = os.path.dirname(__file__)
    for pyfile in os.listdir(_moddir):
        if pyfile.startswith('_') or (not os.path.isdir(_moddir + '/' + pyfile) and not pyfile.endswith('.py')):
            continue
    
        module = pyfile.replace('.py', '')
        imp = __import__('dynamo.web.modules.' + module, globals(), locals(), ['export_data', 'export_web'])
        modules['data'][module] = imp.export_data
        modules['web'][module] = imp.export_web

        if hasattr(imp, 'registry_alias'):
            for alias, mappings in imp.registry_alias.iteritems():
                modules['registry'][alias] = mappings
