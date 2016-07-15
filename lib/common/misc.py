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


class ExceptionHolder(object):
    def __init__(self):
        self.exception = None

    def set(self, exc):
        self.exception = exc


def parallel_exec(target, arguments, add_args = None, get_output = False, per_thread = 1, num_threads = config.num_threads, print_progress = False):
    """
    Execute target(*args) in up to num_threads parallel threads,
    for each entry args of arguments list.
    """

    if len(arguments) == 0:
        if get_output:
            return []
        else:
            return

    def target_wrapper(arguments_chunk, output_list, exception):
        try:
            for args in arguments_chunk:
                output = target(*args)
                if get_output:
                    output_list.append(output)

        except Exception as ex:
            exception.set(ex)

    if print_progress:
        ntotal = len(arguments)
        interval = max(ntotal / 20, 1)

    if get_output:
        all_outputs = []

    if per_thread < 1:
        per_thread = 1

    threads = []
    outputs = []
    exceptions = []
    processing = True

    while processing:
        arguments_chunk = []
        for icall in range(per_thread):
            args = arguments.pop()
            if print_progress and (ntotal - len(arguments)) % interval == 0:
                logging.info('Processed %f%% of input.', 100. * (ntotal - len(arguments)) / ntotal)
    
            if type(args) is not tuple:
                args = (args,)
    
            if add_args is not None:
                args = args + add_args

            arguments_chunk.append(args)

            if len(arguments) == 0:
                processing = False
                break

        thread_outputs = []
        thread_exception = ExceptionHolder()
        thread = threading.Thread(target = target_wrapper, args = (arguments_chunk, thread_outputs, thread_exception))
        thread.name = 'Th%d' % len(threads)

        thread.start()
        threads.append(thread)
        if get_output:
            outputs.append(thread_outputs)
        exceptions.append(thread_exception)

        while len(threads) >= num_threads:
            ith = 0
            while ith < len(threads):
                thread = threads[ith]
                if thread.is_alive():
                    ith += 1
                else:
                    thread.join()
                    exc = exceptions.pop(ith)
                    if exc.exception is not None:
                        print 'Exception in thread ' + thread.name
                        raise exc.exception

                    threads.pop(ith)
                    if get_output:
                        all_outputs.extend(outputs.pop(ith))

            if len(threads) >= num_threads:
                time.sleep(1)

    for ith, thread in enumerate(threads):
        thread.join()
        if get_output:
            all_outputs.extend(outputs[ith])

    if get_output:
        return all_outputs

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
