class ServerHost(object):
    _statuses = ['initial', 'starting', 'online', 'updating', 'outofsync']
    STAT_INITIAL, STAT_STARTING, STAT_ONLINE, STAT_UPDATING, STAT_ERROR, STAT_OUTOFSYNC = range(1, 7)

    @staticmethod
    def status_name(arg):
        try:
            return ServerHost._statuses[arg - 1]
        except:
            return arg

    @staticmethod
    def status_val(arg):
        try:
            return eval('ServerHost.STAT_' + arg.upper())
        except:
            return arg

    def __init__(self, hostname):
        self.hostname = hostname
        self.status = ServerHost.STAT_INITIAL
        self.has_store = False
        self.board = None
