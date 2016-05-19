import threading
import time

import common.configuration as config

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

def parallel_exec(target, arguments):
    """
    Execute target(*args) in up to config.num_threads parallel threads,
    for each entry args of arguments list.
    """

    threads = []
    while len(arguments) != 0:
        args = arguments.pop()
        if type(args) is not tuple:
            args = (args,)

        thread = threading.Thread(target = dbs_check, args = args)
        thread.start()
        threads.append(thread)

        while len(threads) >= config.num_threads:
            for thread in threads:
                if not thread.is_alive():
                    thread.join()
                    threads.remove(thread)
                    break
            else:
                time.sleep(1)

    for thread in threads:
        thread.join()
