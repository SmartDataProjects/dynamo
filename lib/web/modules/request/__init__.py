from . import copy
from . import deletion

export_data = {}
export_data.update(copy.export_data)
export_data.update(deletion.export_data)

export_web = {}

# backward compatibility
registry_alias = {
    'request': export_data
}
