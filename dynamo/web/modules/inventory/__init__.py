from . import datasets
from . import groups
from . import sites
from . import stats
from . import inject
from . import delete
from . import blockreplicas
from . import subscriptions
from . import requestlist

export_data = {}
export_data.update(datasets.export_data)
export_data.update(groups.export_data)
export_data.update(sites.export_data)
export_data.update(stats.export_data)
export_data.update(inject.export_data)
export_data.update(delete.export_data)
export_data.update(blockreplicas.export_data)
export_data.update(subscriptions.export_data)
export_data.update(requestlist.export_data)

export_web = {}
export_web.update(stats.export_web)
