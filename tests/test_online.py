"""
This tests the parts of the system over some sockets
"""
from pymultinode.dispatcher import Dispatcher
from pymultinode.worker import Worker
from pymultinode.processor import Processor
from pymultinode.configuration import ConfigurationLibrary, single_file_configuration
from nose.tools import assert_equals
from pymultinode.protocol import request_protocol_from_socket
from pymultinode.proxy import *
import eventlet

# in order to make pickling work, we stick this function in main
import __main__
__main__.calc = lambda a,b: a * b
__main__.calc.__module__ = '__main__'
__main__.calc.__name__ = 'calc'

PORT = 12349
ADDRESS = ('localhost', PORT)

def test_onine():
    """
    Wire up all the objects using the sockets
    """
    dispatcher = Dispatcher()
    library = ConfigurationLibrary()
    worker = Worker(library)

    server_socket = eventlet.listen( ADDRESS )
    worker_client = eventlet.spawn( eventlet.connect, ADDRESS )
    controller_client = eventlet.spawn( eventlet.connect, ADDRESS )
    
    for x in range(2):
        client_socket, address = server_socket.accept()
        protocol = request_protocol_from_socket(client_socket)
        dispatcher_server = DispatcherServer(dispatcher, protocol)
        configuration_library = ConfigurationLibraryServer(library, protocol)

    def on_conn(thread):
        socket = thread.wait()
        protocol = request_protocol_from_socket(socket)
        dispatcher = DispatcherClient(protocol)
        libraray = ConfigurationLibraryClient(protocol)
        return dispatcher, libraray

    dispatcher1, library1 = on_conn(worker_client)
    dispatcher2, library2 = on_conn(controller_client)

    dispatcher1.add_worker(worker, 1)

    # our configuration is just a python file with an add function in it
    config = single_file_configuration('offline', 
            'def calc(a, b): return a + b')
    library2.add(config)

    processor = Processor(config.hash, dispatcher2)
    # do a bunch of silly adding
    for idx, element in enumerate(processor.imap(__main__.calc, xrange(10), xrange(10, 0, -1))):
        assert_equals(10, element)
