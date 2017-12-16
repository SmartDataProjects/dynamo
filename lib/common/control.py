import signal
import traceback

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

    def __init__(self, signums = [signal.SIGINT, signal.SIGTERM], logger = None):
        self._signums = list(signums) # set of signums
        self._default = {} # {signum: handler}
        self._stacktrace = {} # {signum: str}
        
        self._logger = logger

    def __enter__(self):
        self.block()

    def __exit__(self, exc_type, exc_value, tb):
        self.unblock()

    def handle(self, signum, frame):
        # signum, frame are required arguments
        if self._logger:
            self._logger.warning('The system is in the middle of a critical operation and cannot be interrupted just now.')

        # format_stack returns a list of strings corresponding to the stack trace (one entry per call)
        self._stacktrace[signum] = ''.join(traceback.format_stack(frame))

    def block(self):
        for signum in self._signums:
            # set self.handle as the signal handler and save the original handler in _default
            self._default[signum] = signal.signal(signum, self.handle)
            self._stacktrace[signum] = None

    def unblock(self):
        for signum in self._signums:
            signal.signal(signum, self._default.pop(signum))
    
            stacktrace = self._stacktrace.pop(signum)
            if stacktrace:
                if self._logger:
                    self._logger.error('Process was interrupted by signal %d.\nStack trace at the time of interruption was:' + stacktrace)
    
                raise KeyboardInterrupt('Interrupted by signal %d' % signum)
