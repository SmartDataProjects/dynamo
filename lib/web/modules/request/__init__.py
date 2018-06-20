from . import copy
from . import delete

export_data = {}
export_data.update(copy.export_data)
export_data.update(delete.export_data)

export_web = {}
