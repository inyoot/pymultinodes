"""
This tests the parts of the system over some sockets using handshakes
"""
from pymultinode.processor import Processor
from nose.tools import assert_equals
from pymultinode.configuration import single_file_configuration
from pymultinode.handshake import standard_connect
import eventlet
from pymultinode import process

# in order to make pickling work, we stick this function in main
import __main__
__main__.calc = lambda a,b: a * b
__main__.calc.__module__ = '__main__'
__main__.calc.__name__ = 'calc'

PASSWORD = 'pollo'
PORT = 12341
ADDRESS = ('localhost', PORT)

def test_onine():
    """
    Use the process api to set things up
    """
    eventlet.spawn( process.server_process, ADDRESS, PASSWORD )
    eventlet.sleep()
    eventlet.spawn( process.worker_process, ADDRESS, PASSWORD )

    dispatcher, library = standard_connect( ADDRESS, PASSWORD)
    # our configuration is just a python file with an add function in it
    config = single_file_configuration('offline', 
            'def calc(a, b): return a + b')
    library.add(config)

    processor = Processor(config.hash, dispatcher)
    # do a bunch of silly adding
    for idx, element in enumerate(processor.imap(__main__.calc, xrange(10), xrange(10, 0, -1))):
        assert_equals(10, element)

