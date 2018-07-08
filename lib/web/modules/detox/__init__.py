import monitor
import history
import locks

export_data = {}
export_data.update(history.export_data)
export_data.update(locks.export_data)

export_web = {}
export_web.update(monitor.export_web)
export_web.update(locks.export_web)
