my_modules = {}

from datasets import exports

my_modules.update(exports)

from dynamo.web.modules import modules

modules['inventory'] = my_modules
