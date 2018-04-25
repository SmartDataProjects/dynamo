my_modules = {}

# Import everything exported by files in this directory (repeat the below lines for all files)
from example import exports
my_modules.update(exports)

# Then finally set the mapping to the global modules dict (imported by dynamo.web.server)
from dynamo.web.modules import modules
modules['transfers'] = my_modules
