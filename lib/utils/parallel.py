import time
import multiprocessing
import threading

from dataformat import Configuration

class FunctionWrapper(object):
    def __init__(self, function):
        self.function = function

    def __call__(self, inputs, outputs, exception):
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
    def __init__(self, function, num_threads):
        self.target = FunctionWrapper(function)
        self.num_threads = num_threads

        self.print_progress = False
        self.timeout = 0
        self.repeat_on_exception = False
        self.logger = None

        # if ntotal != 0, we print progress
        self.ntotal = 0
        self._start_time = 0
        self._ndone = 0
        self._watermark = 0

        self._threads = []
        self.outputs = []

    def add_thread(self, inputs, name = ''):
        """
        Create a new thread and start. Block if there are already max_threads number of threads.
        """

        while len(self._threads) >= self.num_threads:
            self.collect()
            time.sleep(1)

        outputs = []
        exception = ExceptionHolder()

        thread = threading.Thread(target = self.target, args = (inputs, outputs, exception))
        thread.daemon = True
        if name:
            thread.name = name
    
        thread.start()
        if self._start_time == 0:
            self._start_time = time.time()

        self._threads.append((thread, time.time(), inputs, outputs, exception))

    def collect(self):
        ith = 0
        while ith < len(self._threads):
            thread, time_started, inputs = self._threads[ith][0:3]

            if thread.is_alive():
                if self.timeout > 0 and time.time() - time_started > self.timeout:
                    if self.logger:
                        self.logger.error('Thread ' + thread.name + ' timed out.')
                        self.logger.error('Inputs: ' + str([str(i) for i in inputs]))

                    raise ThreadTimeout(thread.name)

                ith += 1
                continue

            thread.join()
            thread, time_started, inputs, outputs, exception = self._threads.pop(ith)

            if exception.exception is not None:
                if self.logger:
                    self.logger.error('Exception in thread ' + thread.name)
                    self.logger.error('Inputs: ' + str([str(i) for i in inputs]))

                if self.repeat_on_exception:
                    if self.logger:
                        self.logger.error('Repeating execution')

                    for args in inputs:
                        self.target.function(*args) # no catch

                    if self.logger:
                        self.logger.error('No exception was thrown during the repeat.')

                raise exception.exception

            self.outputs.extend(outputs)

            if self.ntotal != 0 and self.logger: # progress report requested
                self._ndone += len(inputs)
                if self._ndone == self.ntotal or self._ndone > self._watermark:
                    self.logger.info('Processed %.1f%% of input (%ds elapsed).', 100. * self._ndone / self.ntotal, int(time.time() - self._start_time))
                    self.watermark += max(1, self._ntotal / 20)

class Map(object):
    """
    Similar to multiprocessing.Pool.map but with threads. At each execute() call, instantiate a ThreadCollector
    object to do the real work. Output list can be out of order.
    """

    def __init__(self, config = Configuration()):
        self.num_threads = max(config.get('num_threads', multiprocessing.cpu_count() - 1), 1)
        self.task_per_thread = config.get('task_per_thread', 1)

        self.print_progress = config.get('print_progress', False)
        self.timeout = config.get('timeout', 0)
        self.repeat_on_exception = config.get('repeat_on_exception', True)

        self.logger = None

    def execute(self, function, arguments):
        if len(arguments) == 0:
            return []
    
        collector = ThreadCollector(function, self.num_threads)
       
        collector.print_progress = self.print_progress
        collector.timeout = self.timeout
        collector.repeat_on_exception = self.repeat_on_exception
        collector.logger = self.logger

        if self.print_progress:
            collector.ntotal = len(arguments)

        # format the inputs: list (one element for one thread) of lists (arguments x per_thread) of tuples
        input_list = [[]]
        for args in arguments:
            if type(args) is not tuple:
                args = (args,)
    
            input_list[-1].append(args)
            if len(input_list[-1]) == self.task_per_thread:
                input_list.append([])
    
        for inputs in input_list:
            collector.add_thread(inputs)

        collector.collect()
    
        return collector.outputs
