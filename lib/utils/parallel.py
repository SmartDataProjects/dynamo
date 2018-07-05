import time
import multiprocessing
import threading
import Queue

from dynamo.dataformat import Configuration

class FunctionWrapper(object):
    def __init__(self, function, start_sem, done_sem):
        self.function = function
        self._start_sem = start_sem
        self._done_sem = done_sem

    def __call__(self, inputs, outputs, exception):
        try:
            for args in inputs:
                output = self.function(*args)
                outputs.append(output)

        except Exception as ex:
            exception.set(ex)

        self._start_sem.release()
        self._done_sem.release()


class ExceptionHolder(object):
    def __init__(self):
        self.exception = None

    def set(self, exc):
        self.exception = exc


class ThreadTimeout(RuntimeError):
    pass


class ThreadController(object):
    def __init__(self, function, start_sem):
        self.print_progress = False
        self.timeout = 0
        self.repeat_on_exception = False
        self.auto_start = False
        self.logger = None

        # if ntotal != 0, we print progress
        self.ntotal = 0
        self._start_time = 0
        self._ndone = 0
        self._watermark = 0

        self._start_sem = start_sem
        self._done_sem = threading.Semaphore(0)
        self._target_function = function

        self._inputs = []
        self._all_outputs = []
        self._thread_defs = []

    def add_inputs(self, arguments, name = ''):
        """
        Reserve a thread.
        @param arguments  List of arguments. Each element corresponds to a single function call
                          within a thread.
        @param name       Name of the thread.
        """

        self._inputs.append((arguments, name))

        if self.auto_start:
            self._collect_threads()
            while len(self._inputs) > 1 and self._start_one(blocking = False):
                pass

    def execute(self):
        """Run all threads and return the full list of outputs."""

        while len(self._inputs) != 0:
            self._start_one()
            self._collect_threads()

        while len(self._thread_defs) != 0:
            self._collect_threads(blocking = True)

        outputs = self._all_outputs

        self._start_time = 0
        self._ndone = 0
        self._watermark = 0
        self._inputs = []
        self._all_outputs = []

        return outputs

    def iterate(self):
        """Run threads and yield the outputs as they become available."""

        while True:
            while len(self._inputs) != 0:
                if not self._start_one(blocking = False):
                    # could not start
                    break

            if len(self._all_outputs) == 0:
                if len(self._inputs) == 0 and len(self._thread_defs) == 0:
                    # we are done
                    return

                # otherwise block until there is a thread completed
                self._collect_threads(blocking = True)

            else:
                yield self._all_outputs.pop()

    def _start_one(self, blocking = True):
        # If blocking = True, return False immediately unless there is a free slot
        if not blocking:
            if not self._start_sem.acquire(False):
                return False

            self._start_sem.release()

        arguments, name = self._inputs.pop(0)

        exception = ExceptionHolder()
        outputs = []

        target = FunctionWrapper(self._target_function, self._start_sem, self._done_sem)
        thread = threading.Thread(target = target, args = (arguments, outputs, exception))
        thread.daemon = True
        if name:
            thread.name = name

        # Blocks here until there is a free slot
        self._start_sem.acquire()
        thread.start()
        if self._start_time == 0:
            self._start_time = time.time()

        self._thread_defs.append((thread, time.time(), arguments, exception, outputs))

        return True

    def _collect_threads(self, blocking = False):
        if blocking:
            # Block until there is a completed thread
            self._done_sem.acquire()
            # Increment the semaphore count by one (to be reduced below)
            self._done_sem.release()

        ith = 0
        while ith != len(self._thread_defs):
            thread, start_time, arguments, exception, outputs = self._thread_defs[ith]
            if self._collect_one(thread, start_time, arguments, exception):
                self._thread_defs.pop(ith)
                self._all_outputs.extend(outputs)
                # Reduce the semaphore count by one
                self._done_sem.acquire()
            else:
                ith += 1

    def _collect_one(self, thread, time_started, inputs, exception):
        if thread.is_alive():
            if self.timeout > 0 and time.time() - time_started > self.timeout:
                if self.logger:
                    self.logger.error('Thread ' + thread.name + ' timed out.')
                    self.logger.error('Inputs: ' + str([str(i) for i in inputs]))

                raise ThreadTimeout(thread.name)

            return False

        thread.join()

        if exception.exception is not None:
            if self.logger:
                self.logger.error('Exception in thread ' + thread.name)
                self.logger.error('Inputs: ' + str([str(i) for i in inputs]))

            if self.repeat_on_exception:
                if self.logger:
                    self.logger.error('Repeating execution')

                for args in inputs:
                    self._target_function(*args) # no catch

                if self.logger:
                    self.logger.error('No exception was thrown during the repeat.')

            raise exception.exception

        if self.ntotal != 0 and self.logger: # progress report requested
            self._ndone += len(inputs)
            if self._ndone == self.ntotal or self._ndone > self._watermark:
                self.logger.info('Processed %.1f%% of input (%ds elapsed).', 100. * self._ndone / self.ntotal, int(time.time() - self._start_time))
                self.watermark += max(1, self._ntotal / 20)

        return True


class AutoStarter(object):
    def __init__(self, function, start_sem, task_per_thread):
        self.inputs = []
        self.controller = ThreadController(function, start_sem)
        self.controller.auto_start = True
        self.task_per_thread = task_per_thread

    def add_input(self, args):
        if type(args) is not tuple:
            args = (args,)

        self.inputs.append(args)

        if len(self.inputs) == self.task_per_thread:
            self.controller.add_inputs(self.inputs)
            del self.inputs[:]

    def close(self):
        if len(self.inputs) != 0:
            self.controller.add_inputs(self.inputs)
            del self.inputs[:]

    def get_outputs(self):
        return self.controller.execute()

    
class Map(object):
    """
    Similar to multiprocessing.Pool.map but with threads. At each execute() call, instantiate a ThreadController
    object to do the real work. Output list can be out of order.
    """

    def __init__(self, config = Configuration()):
        self.start_sem = threading.Semaphore(min(config.get('num_threads', multiprocessing.cpu_count() - 1), 1))
        self.task_per_thread = config.get('task_per_thread', 1)

        self.print_progress = config.get('print_progress', False)
        self.timeout = config.get('timeout', 0)
        self.repeat_on_exception = config.get('repeat_on_exception', True)

        self.logger = None

    def execute(self, function, arguments, async = False):
        """
        Execute function on each argument and return the function outputs in a list.
        The output is not ordered.
        @param function   Thread function.
        @param arguments  List of arguments. Each element corresponds to a single function call.
                          Each element can be a single object or a tuple which gets unpacked.
        @param async      If True, use the iterate function of ThreadController.
        @return Unordered list of function outputs.
        """

        if len(arguments) == 0:
            return []

        controller = ThreadController(function, self.start_sem)
       
        controller.print_progress = self.print_progress
        controller.timeout = self.timeout
        controller.repeat_on_exception = self.repeat_on_exception
        controller.logger = self.logger

        if self.print_progress:
            controller.ntotal = len(arguments)

        # In case we want to run the function multiple times in a single thread, we make slices of inputs
        inputs = []
        for args in arguments:
            if type(args) is not tuple:
                args = (args,)

            inputs.append(args)
            if len(inputs) == self.task_per_thread:
                controller.add_inputs(inputs)
                inputs = []
    
        if len(inputs) != 0:
            controller.add_inputs(inputs)

        if async:
            return controller.iterate()
        else:
            return controller.execute()

    def get_starter(self, function):
        starter = AutoStarter(function, self.start_sem, self.task_per_thread)
        starter.controller.timeout = self.timeout
        starter.controller.repeat_on_exception = self.repeat_on_exception
        starter.controller.logger = self.logger

        return starter
