import monitor
import history
import locks

export_data = {}
export_data.update(history.export_data)
export_data.update(locks.export_data)

export_web = {}
export_web.update(monitor.export_web)
export_web.update(locks.export_web)

# backward compatibility
registry_alias = {
    'detoxlock': {
        'lock': locks.export_data['lock/lock'],
        'unlock': locks.export_data['lock/unlock'],
        'list': locks.export_data['lock/list'],
        'set': locks.export_data['lock/set']
}
