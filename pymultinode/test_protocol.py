"""
test for the protocol
"""
import os
import eventlet
from nose.tools import assert_equals, assert_raises
import socket
from mock import Mock

from .protocol import Protocol, MESSAGE_HEADER, RequestProtocol, ConnectionLost

class PipeFile(object):
    def __init__(self):
        self._waiting = ''
        self._incoming = eventlet.queue.LightQueue()
        self._contents = ''
        self._closed = False

    def write(self, content):
        self._waiting += content

    def flush(self):
        self.other._incoming.put(self._waiting)
        self._waiting = ''

    def close(self):
        self.flush()
        self._closed = True
        self.other._incoming.put(None)

    def read(self, length):
        if self._closed:
            return ''
        while length > len(self._contents):
            item = self._incoming.get()
            if item is None:
                self._closed = True
                return self._contents
            else:
                self._contents += item

        result = self._contents[:length]
        self._contents = self._contents[length:]
        return result



def create_pipes():
    """
    Create a couple of pipes and return them
    """
    file1 = PipeFile()
    file2 = PipeFile()
    file1.other = file2
    file2.other = file1
    return file1, file2

def quick_protocols():
    read, write = create_pipes()
    protocol1 = Protocol(read)
    protocol2 = Protocol(write)
    return protocol1, protocol2

def test_simple():
    """
    Make sure the same message comes out the other end of the pipe
    """
    protocol, protocol2 = quick_protocols()
    protocol.send('X', 24, 'alpha')
    assert_equals( ('X', 24, 'alpha'), protocol2.wait() )

def test_empty():
    """
    Make sure that a closed connection gives us None
    """
    read, write = create_pipes()
    write.close()
    protocol2 = Protocol(read)
    assert_equals( None, protocol2.wait() )

def test_truncated():
    """
    Make sure a truncated message gives us None
    """
    read, write = create_pipes()
    write.write( MESSAGE_HEADER.pack('X', 4, 2) )
    write.write( '1' )
    write.flush()
    write.close()
    protocol2 = Protocol(read)
    assert_equals( None, protocol2.wait() )

def quick_request(callback2 = None):
    """
    Build a couple of request protocols
    """
    protocol1, protocol2 = quick_protocols()
    protocol1 = RequestProtocol(protocol1)
    protocol2 = RequestProtocol(protocol2)
    if callback2 is not None:
        protocol2.register_handler('X', callback2)

    return protocol1, protocol2

def test_request():
    """
    Make sure that the request/response system works
    """
    def callback(command, sequence, data):
        assert_equals( ('X', 0, 'monkeys'), (command, sequence, data) )
        protocol2.respond( 0, 'banana' )
    protocol1, protocol2 = quick_request(callback)
    event = protocol1.request('X', 'monkeys')
    assert_equals( 'banana', event.wait() )


def test_command():
    """
    Make sure that we can submit commands
    """
    def callback(command, sequence, data):
        assert_equals( ('X', 0, 'monkeys'), (command, sequence, data) )

    protocol1, protocol2 = quick_request(callback)
    protocol1.command('X', 'monkeys')

def test_test_reject_old():
    client, server=  quick_request( lambda a,b,c:None )
    event = client.request('X', 'mothballs')
    server.close()

    assert_raises(ConnectionLost, event.wait)

def test_test_reject_new():
    client, server=  quick_request( lambda a,b,c:None )
    server.close()
    eventlet.sleep()

    assert_raises(ConnectionLost, client.request, 'X', 'monkeys')

def test_invalid_command():
    client, server=  quick_request( lambda a,b,c:None)
    client.request('B', 'bad')

def test_socket_error():
    event = eventlet.event.Event()
    protocol = Mock(Protocol)
    protocol.send.side_effect = socket.error()
    protocol.wait = event.wait
    request_protocol = RequestProtocol(protocol)
    assert_raises(ConnectionLost, request_protocol.command, 'X', 'milk')
    
def test_ioerror():
    event = eventlet.event.Event()
    protocol = Mock(Protocol)
    protocol.send.side_effect = IOError
    protocol.wait = event.wait
    request_protocol = RequestProtocol(protocol)
    assert_raises(ConnectionLost, request_protocol.request, 'X', 'milk')
 
