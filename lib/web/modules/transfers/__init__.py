import current
import history
import monitor
import held

export_data = {}
export_data.update(current.export_data)
export_data.update(history.export_data)
export_data.update(held.export_data)

export_web = {}
export_web.update(monitor.export_web)
