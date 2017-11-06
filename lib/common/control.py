import logging
import signal

class SigintHandler(object):
    """
    Block SIGINT during critical operations.
    """

    def __init__(self):
        self._blocking = False
        self._interrupted = False
        self._default = signal.getsignal(signal.SIGINT)

    def __call__(self, signum, frame):
        logging.warning('The system is in the middle of a critical operation and cannot be interrupted just now.')
        self._interrupted = True

    def block(self):
        if self._blocking:
            logging.error('SIGINT is already being blocked.')
            return

        self._default = signal.signal(signal.SIGINT, self)
        self._blocking = True

    def unblock(self):
        signal.signal(signal.SIGINT, self._default)

        if self._interrupted:
            raise KeyboardInterrupt()

        self._interrupted = False
        self._blocking = False

sigint = SigintHandler()
