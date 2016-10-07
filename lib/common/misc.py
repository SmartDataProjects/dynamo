import threading
import time
import logging
import signal
from functools import wraps

import common.configuration as config

def timer(function):
    @wraps(function)
    def function_timer(*args, **kwargs):
        t0 = time.time()
        result = function(*args, **kwargs)
        t1 = time.time()
        if config.show_time_profile:
            logging.info('Wall-clock time for executing %s: %.1fs', function.func_name, t1 - t0)

        return result

    return function_timer


def unicode2str(container):
    """
    Recursively convert unicode values in a nested container to strings.
    """

    if type(container) is list:
        for idx in range(len(container)):
            elem = container[idx]

            if type(elem) is unicode:
                container[idx] = str(elem)

            elif type(elem) is dict or type(elem) is list:
                unicode2str(elem)

    elif type(container) is dict:
        keys = container.keys()
        for key in keys:
            elem = container[key]

            if type(key) is unicode:
                container.pop(key)
                key = str(key)
                container[key] = elem

            if type(elem) is unicode:
                container[key] = str(elem)

            elif type(elem) is dict or type(elem) is list:
                unicode2str(elem)


class FunctionWrapper(object):
    def __init__(self, function):
        self.function = function
        self.time_started = 0

    def __call__(self, inputs, outputs, exception):
        self.time_started = time.time()
        try:
            for args in inputs:
                output = self.function(*args)
                outputs.append(output)

        except Exception as ex:
            exception.set(ex)


class ExceptionHolder(object):
    def __init__(self):
        self.exception = None

    def set(self, exc):
        self.exception = exc


class ThreadTimeout(RuntimeError):
    pass


class ThreadCollector(object):
    def __init__(self, ntotal = 0, timeout = 0):
        self.outputs = []
        self.ntotal = ntotal
        self.ndone = 0
        self.watermark = 0
        self.timeout = timeout

    def collect(self, threads):
        ith = 0
        while ith < len(threads):
            thread, time_started = threads[ith][0:2]
            if thread.is_alive():
                if self.timeout > 0 and time.time() - time_started > self.timeout:
                    logger.error('Thread ' + thread.name + ' timed out.')
                    raise ThreadTimeout(thread.name)

                ith += 1
                continue

            thread.join()
            thread, time_started, inputs, outputs, exception = threads.pop(ith)
            if exception.exception is not None:
                logging.error('Exception in thread ' + thread.name)
                raise exception.exception

            self.outputs.extend(outputs)

            if self.ntotal != 0: # progress report requested
                self.ndone += len(inputs)
                if self.ndone == self.ntotal or self.ndone > self.watermark:
                    logging.info('Processed %.1f%% of input.', 100. * self.ndone / self.ntotal)
                    self.watermark += max(1, self.ntotal / 20)


def parallel_exec(function, arguments, per_thread = 1, num_threads = config.num_threads, print_progress = False, timeout = 0):
    """
    Execute function(*args) in up to num_threads parallel threads,
    for each entry args of arguments list.
    """

    if len(arguments) == 0:
        return []

    if per_thread < 1:
        per_thread = 1

    threads = []
    processing = True

    target = FunctionWrapper(function)
    if print_progress:
        collector = ThreadCollector(len(arguments), timeout = timeout)
    else:
        collector = ThreadCollector(timeout = timeout)

    # format the inputs: list (one element for one thread) of lists (arguments x per_thread) of tuples
    input_list = [[]]
    for args in arguments:
        if type(args) is not tuple:
            args = (args,)

        input_list[-1].append(args)
        if len(input_list[-1]) == per_thread:
            input_list.append([])

    for inputs in input_list:
        outputs = []
        exception = ExceptionHolder()
        thread = threading.Thread(target = target, args = (inputs, outputs, exception))
        thread.name = 'Th%d' % len(threads)
        thread.daemon = True

        thread.start()
        threads.append((thread, time.time(), inputs, outputs, exception))

        while len(threads) >= num_threads:
            collector.collect(threads)
            time.sleep(1)

    while len(threads) > 0:
        collector.collect(threads)
        time.sleep(1)

    return collector.outputs


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
