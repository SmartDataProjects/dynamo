import threading
import time
import logging
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

def parallel_exec(target, arguments, clean_input = True, print_progress = False):
    """
    Execute target(*args) in up to config.num_threads parallel threads,
    for each entry args of arguments list.
    """

    ntotal = len(arguments)

    threads = []
    iarg = 0
    while iarg != len(arguments):
        if clean_input:
            args = arguments.pop()
            if print_progress and (ntotal - len(arguments)) % (ntotal / 20) == 0:
                logging.info('Processed %f%% of input.', 100. * (ntotal - len(arguments)) / ntotal)
        else:
            args = arguments[iarg]
            if print_progress and iarg % (ntotal / 20) == 0:
                logging.info('Processed %f%% of input.', 100. * iarg / ntotal)

            iarg += 1

        if type(args) is not tuple:
            args = (args,)

        thread = threading.Thread(target = target, args = args)
        thread.start()
        threads.append(thread)

        while len(threads) >= config.num_threads:
            iL = 0
            while iL < len(threads):
                thread = threads[iL]
                if thread.is_alive():
                    iL += 1
                else:
                    thread.join()
                    threads.pop(iL)

            if len(threads) >= config.num_threads:
                time.sleep(1)

    for thread in threads:
        thread.join()
