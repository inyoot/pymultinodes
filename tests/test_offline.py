"""
This tests the parts of the system without actually going over any sockets
"""
from pymultinode.dispatcher import Dispatcher
from pymultinode.worker import Worker
from pymultinode.processor import Processor
from pymultinode.configuration import Configuration, ConfigurationLibrary, single_file_configuration
from nose.tools import assert_equals

# in order to make pickling work, we stick this function in main
import __main__
__main__.calc = lambda a,b: a * b
__main__.calc.__module__ = '__main__'
__main__.calc.__name__ = 'calc'

def test_offline():
    """
    Wire up all the objects without using any sockets
    """
    dispatcher = Dispatcher()
    library = ConfigurationLibrary()
    worker = Worker(library)
    dispatcher.add_worker(worker, 1)

    # our configuration is just a python file with an add function in it
    config = single_file_configuration('offline', 
            'def calc(a, b): return a + b')
    library.add(config)

    processor = Processor(config.hash, dispatcher)
    # do a bunch of silly adding
    for idx, element in enumerate(processor.imap(__main__.calc, xrange(10), xrange(10, 0, -1))):
        assert_equals(10, element)

