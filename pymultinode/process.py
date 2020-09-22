"""
pymultinode.process

functions to start different types of processes
"""
from .configuration import ConfigurationLibrary
from .dispatcher import Dispatcher
from .handshake import ConnectionHandler, WebServer, standard_connect
from .web import WebRequestHandler
from .worker import Worker
from multiprocessing import cpu_count, Process
import sys
import os

def server_process(address, secret):
    dispatcher = Dispatcher()
    library = ConfigurationLibrary()
    worker = Worker(library)
    dispatcher.add_worker(worker, cpu_count() )
    connection_handler = ConnectionHandler(dispatcher, library, secret)
    web_request_handler = WebRequestHandler(dispatcher)
    web_server = WebServer(web_request_handler.handle_web_request, connection_handler)
    web_server.listen(address)

def unix_worker_process(address, secret):
    dispatcher, library = standard_connect(address, secret)
    worker = Worker(library)
    dispatcher.add_worker(worker, cpu_count())
    dispatcher._protocol.wait_shutdown()

def single_worker_process(address, secret):
    dispatcher, library = standard_connect(address, secret)
    worker = Worker(library)
    dispatcher.add_worker(worker, 1)
    dispatcher._protocol.wait_shutdown()


def win32_worker_process(address, secret):
    """
    eventlet doesn't provide non-blocking IO for subprocess on windows
    This makes things not work so well. In order to combat that, on windows
    worker starts up several workers each of which only has one slave
    """
    procs = [Process(target = single_worker_process, args = (address, secret))
                for cpu in xrange(cpu_count())]
    for proc in procs:
        proc.start()
    for proc in procs:
        proc.join()



def worker_process(address, secret):
    multinode_path = os.path.abspath( os.path.join( os.path.dirname(__file__), '..') )
    print "Multinode location is ", multinode_path
    os.chdir( multinode_path )
    if sys.platform == 'win32':
        win32_worker_process(address, secret)
    else:
        unix_worker_process(address, secret)


