"""
pymultinode.protocol

This code provides the basic protocol which is used to communicate
"""
import struct
import eventlet
import socket

class CommandCodes:
    Response = 'R'
    AddConfiguration = 'A'
    GetConfiguration = 'G'
    RemoveConfiguration = 'D'
    WorkerTask = 'W'
    AddWorker = 'C'
    DispatchTask = 'T'

class ConnectionLost(Exception):
    pass

MESSAGE_HEADER = struct.Struct('!cLL')
class Protocol(object):
    """
    The Protocol used to communicate

    Each message consists of:
        command code - 1 byte
        sequence number - 4 bytes
        length - 4 bytes

        data - n bytes

    """
    def __init__(self, file):
        self._file = file
        self._lock = eventlet.semaphore.Semaphore()

    def close(self):
        """
        Close the connection
        """
        self._file.close()

    def send(self, command, sequence, data):
        """
        Transmit the message over the wire
        """
        encoded = MESSAGE_HEADER.pack(command, sequence, len(data) )
        with self._lock:
            self._file.write( encoded )
            self._file.write(data)
            self._file.flush()

    def wait(self):
        """
        Return a single message from the wire possibly waiting for it

        if this function returns None, the connection has been closed
        """
        encoded = self._file.read( MESSAGE_HEADER.size )
        # if we ever cannot read the full amount
        # we have closed in the middle of the connection
        # report it as a lost connection
        if len(encoded) != MESSAGE_HEADER.size:
            return None

        command, sequence, length = MESSAGE_HEADER.unpack(encoded)
        data = self._file.read(length)
        # only report success if all data is there
        if len(data) != length:
            return None
        return command, sequence, data

class RequestProtocol(object):
    """
    The RequestProtocol builds on the Protocol to provide support for
    requests.
    """
    def __init__(self, protocol):
        """
        Construcst a RequestProtocol

        protocol should be a Protocol
        callback should be a function taking three argumenst
            command code, sequence, data
        these will indicate request that come over the wire.
        When the connection closes these will be called with all Nones
        """
        self._protocol = protocol
        self._counter = 0
        self._events = {}
        self._alive = True
        self._handlers = {}
        self._thread= eventlet.spawn(self._process)

    def register_handler(self, command, callback):
        self._handlers[command] = callback

    def _cleanup(self):
        self._alive = False
        for event in self._events.values():
            event.send_exception( ConnectionLost() )

    def _process(self):
        """
        This thread takes care of reading the message and dealing
        with them
        """
        while True:
            result = self._protocol.wait()
            # result = None indicates that the connection is
            # closed
            if result is None:
                self._cleanup()
                break

            command, sequence, data  = result
            # for responses we signal the events
            # otherwise we allow the callback to worry about it
            if command == CommandCodes.Response:
                event = self._events.pop(sequence)
                event.send(data)
            else:
                try:
                    self._handlers[command](command, sequence, data)
                except KeyError:
                    # drop anybody who tries bad commands
                    self.close()


    def request(self, command, data):
        """
        Make a request of type command with data
        """
        if not self._alive:
            raise ConnectionLost()
        request_id = self._counter
        self._counter += 1

        event = eventlet.event.Event()
        self._events[request_id] = event

        self._send(command, request_id, data)

        return event

    def respond(self, sequence, data):
        """
        Send a response to a previous request
        """
        self._send(CommandCodes.Response, sequence, data)

    def command(self, command, data):
        """
        Submit a command
        """
        request_id = self._counter
        self._counter += 1
        self._send(command, request_id, data)
        
    def _send(self, command, sequence, data):
        try:
            self._protocol.send(command, sequence, data)
        except (socket.error, IOError):
            raise ConnectionLost()


    def close(self):
        """
        Close the connection
        """
        self._protocol.close()

    def wait_shutdown(self):
        self._thread.wait()


def request_protocol_from_file(file):
    """
    Construct a protocol talking to a file
    """
    protocol = Protocol(file)
    return RequestProtocol(protocol)

def request_protocol_from_socket(socket):
    """
    Construct a protocol talking to a socket
    """
    return request_protocol_from_file( socket.makefile('rw') )
