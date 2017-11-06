import threading
import time
import logging
from functools import wraps

from common.configuration import common_config


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
    def __init__(self, target, ntotal = 0, timeout = 0):
        self.target = target
        self.outputs = []
        self.ntotal = ntotal
        self.ndone = 0
        self.watermark = 0
        self.start_time = 0
        self.timeout = timeout

    def collect(self, threads):
        if self.ntotal != 0 and self.start_time == 0:
            self.start_time = time.time()

        ith = 0
        while ith < len(threads):
            thread, time_started, inputs = threads[ith][0:3]
            if thread.is_alive():
                if self.timeout > 0 and time.time() - time_started > self.timeout:
                    logging.error('Thread ' + thread.name + ' timed out.')
                    logging.error('Inputs: ' + str([str(i) for i in inputs]))
                    raise ThreadTimeout(thread.name)

                ith += 1
                continue

            thread.join()
            thread, time_started, inputs, outputs, exception = threads.pop(ith)
            if exception.exception is not None:
                logging.error('Exception in thread ' + thread.name)
                logging.error('Inputs: ' + str([str(i) for i in inputs]))

                if common_config.debug.repeat_failed_thread:
                    logging.error('Repeating execution')
                    for args in inputs:
                        self.target.function(*args) # no catch

                    logging.error('No exception was thrown during the repeat.')

                raise exception.exception

            self.outputs.extend(outputs)

            if self.ntotal != 0: # progress report requested
                self.ndone += len(inputs)
                if self.ndone == self.ntotal or self.ndone > self.watermark:
                    logging.info('Processed %.1f%% of input (%ds elapsed).', 100. * self.ndone / self.ntotal, int(time.time() - self.start_time))
                    self.watermark += max(1, self.ntotal / 20)


def parallel_exec(function, arguments, per_thread = 1, num_threads = common_config.general.threads, print_progress = False, timeout = 0):
    """
    Execute function(*args) in up to num_threads parallel threads,
    for each entry args of arguments list.
    """

    if len(arguments) == 0:
        return []

    if num_threads <= 1:
        outputs = []
        for args in arguments:
            if type(args) is tuple:
                outputs.append(function(*args))
            else:
                outputs.append(function(args))

        return outputs

    if per_thread < 1:
        per_thread = 1

    threads = []
    processing = True

    target = FunctionWrapper(function)
    if print_progress:
        collector = ThreadCollector(target, ntotal = len(arguments), timeout = timeout)
    else:
        collector = ThreadCollector(target, timeout = timeout)

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

