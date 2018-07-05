import datasets
import groups
import stats
import inject
import delete

export_data = {}
export_data.update(datasets.export_data)
export_data.update(groups.export_data)
export_data.update(stats.export_data)
export_data.update(inject.export_data)
export_data.update(delete.export_data)

export_web = {}
export_web.update(stats.export_web)
