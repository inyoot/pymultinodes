from .test_protocol import quick_request
from .configuration import ConfigurationLibrary
from .test_configuration import NullConfiguration
from .test_dispatcher import DoubleWorker, GiveUpWorker
from .proxy import ConfigurationLibraryServer, ConfigurationLibraryClient
from .proxy import WorkerServer, WorkerClient
from .proxy import DispatcherServer, DispatcherClient
from nose.tools import assert_equals, assert_raises
from mock import Mock, sentinel
from .dispatcher import Dispatcher
import eventlet

def create_libraries():
    client, server = quick_request()
    library = ConfigurationLibrary()
    library_server = ConfigurationLibraryServer(library, server)
    library_client = ConfigurationLibraryClient(client)

    return library, library_client


def test_library_proxy():
    library, client = create_libraries()
    config = NullConfiguration('alpha')
    client.add(config)
    eventlet.sleep()

    assert_equals( config.hash, library.get(config.hash).hash )

def test_library_proxy_download():
    library, client = create_libraries()
    config = NullConfiguration('alpha')
    library.add(config)
    eventlet.sleep()

    assert_equals( config.hash, client.get(config.hash).hash )

def test_library_proxy_remove():
    library, client = create_libraries()
    config = NullConfiguration('alpha')
    library.add(config)
    client.remove(config.hash)
    eventlet.sleep()
    
    assert_raises( KeyError, library.get, config.hash )


def test_worker_proxy():
    client, server = quick_request()
    worker = DoubleWorker()
    worker_server = WorkerServer(worker, server)
    worker_client = WorkerClient(client)

    assert_equals( 'alphaalpha', worker_client.do_task(None, 'alpha') )

def test_dispatcher_proxy_add_worker():
    client, server = quick_request()
    dispatcher = Mock(Dispatcher)
    dispatcher_server = DispatcherServer(dispatcher, server)
    dispatcher_client = DispatcherClient(client)

    dispatcher_client.add_worker(sentinel.worker, 7)
    eventlet.sleep()

    assert_equals(7, dispatcher.add_worker.call_args[0][-1])


def test_dispatcher_proxy_do_task():
    client, server = quick_request()
    dispatcher = Mock(Dispatcher)
    dispatcher_server = DispatcherServer(dispatcher, server)
    dispatcher_client = DispatcherClient(client)

    my_event = eventlet.event.Event()
    dispatcher.do_task.return_value = my_event

    event = dispatcher_client.do_task('fred', 'red')
    eventlet.sleep()

    dispatcher.do_task.assert_called_with('fred', 'red')
    my_event.send('blue')

    assert_equals( 'blue', event.wait() )




