import sys
import code

from dynamo.core.executable import inventory

class DynamoConsole(code.InteractiveConsole):
    """
    Interactive console with inventory in the namespace.
    """

    def __init__(self):
        mylocals = {'__builtins__': __builtins__, '__name__': '__main__', '__doc__': None, '__package__': None, 'inventory': self.inventory}
        code.InteractiveConsole.__init__(self, locals = mylocals)


class SocketDynamoConsole(DynamoConsole):
    """
    Console where input comes from a socket.
    """

    def __init__(self, conn):
        DynamoConsole.__init__(self)
        self._conn = conn
        self._lines = []
        self._last_line = ''
        
        self._buffer = ''
        self._expected_length = ''

    def write(self, data):
        sys.stderr.write(data)
        try:
            sys.stderr.flush()
        except:
            pass
        
    def raw_input(self, prompt = ''):
        sys.stdout.write(prompt)
        try:
            sys.stdout.flush()
        except:
            return ''

        data = ''

        while len(self._lines) == 0 or len(data) != 0:
            if len(data) == 0:
                # receive data chunk
                chunk = self._conn.recv(2048)
                if not chunk:
                    # socket closed
                    raise EOFError()
    
                data += chunk

            if len(self._buffer) == 0:
                # if we are at the beginning of the chunk
                pos = data.find(' ')
                if pos == -1:
                    # received chunk is not even the full word for the data length
                    self._expected_length += data
                    continue

                self._expected_length += data[:pos]
                data = data[pos + 1:]

            expected_length = int(self._expected_length)

            if expected_length == 0:
                self._expected_length = ''
                raise EOFError()

            # read the data into buffer
            read_length = expected_length - len(self._buffer)
            self._buffer += data[:read_length]

            # shift data
            data = data[read_length:]

            if len(self._buffer) < expected_length:
                # data didn't contain the full content
                continue

            # now we have the buffer with intended length
            # note that we don't guarantee the buffer ends nicely with a newline
            # i.e. the buffer may say it's 30 characters long and send 30 characters,
            # but may not be the whole command line

            # split buffer into lines
            while True:
                newline = self._buffer.find('\n')
                if newline == -1:
                    self._last_line += self._buffer
                    break
                else:
                    self._last_line += self._buffer[:newline]
                    self._lines.append(self._last_line)
                    self._last_line = ''
                    self._buffer = self._buffer[newline + 1:]

            self._expected_length = ''

        return self._lines.pop(0)
                    
