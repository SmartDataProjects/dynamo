import signal

class SignalConverter(object):
    """
    Convert signals to KeyboardInterrupt.
    """

    def __init__(self, logger = None):
        self._converted = set() # set of signums
        self._default = {} # {signum: handler}

        self._logger = logger

    def __call__(self, signum, frame):
        # signum, frame are required arguments

        raise KeyboardInterrupt('Interrupted by signal %d' % signum)

    def set(self, signum):
        if signum in self._converted:
            if self._logger:
                self._logger.error('Signal %d is already being converted.', signum)

            return

        # set self as the signal handler and save the original handler in _default
        self._default[signum] = signal.signal(signum, self)
        self._converted.add(signum)

    def unset(self, signum):
        signal.signal(signum, self._default.pop(signum))

        self._converted.remove(signum)


class SignalBlocker(object):
    """
    Block signals during critical operations.
    """

    def __init__(self, logger = None):
        self._blocking = set() # set of signums
        self._interrupted = {} # {signum: bool}
        self._default = {} # {signum: handler}

        self._logger = logger

    def __call__(self, signum, frame):
        # signum, frame are required arguments

        if self._logger:
            self._logger.warning('The system is in the middle of a critical operation and cannot be interrupted just now.')

        self._interrupted[signum] = True

    def block(self, signum):
        if signum in self._blocking:
            if self._logger:
                self._logger.error('Signal %d is already being blocked.', signum)

            return

        # set self as the signal handler and save the original handler in _default
        self._default[signum] = signal.signal(signum, self)
        self._blocking.add(signum)
        self._interrupted[signum]= False

    def unblock(self, signum):
        signal.signal(signum, self._default.pop(signum))

        if self._interrupted.pop(signum):
            raise KeyboardInterrupt('Interrupted by signal %d' % signum)

        self._blocking.remove(signum)
