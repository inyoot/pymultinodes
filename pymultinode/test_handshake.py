from mock import Mock
from test_protocol import create_pipes
from .dispatcher import Dispatcher
from .configuration import ConfigurationLibrary
import eventlet
from .handshake import ConnectionHandler, client_handshake, WebServer, client_web_connect
from nose.tools import assert_raises, assert_equals
from .protocol import ConnectionLost
from eventlet.green import urllib2



def test_server_handshake():
    server, client = create_pipes()
    dispatcher = Mock(Dispatcher)
    library = Mock(ConfigurationLibrary)
    handler = ConnectionHandler(dispatcher, library, 'secret')

    thread = eventlet.spawn( handler.new_connection, server )

    client_dispatcher, client_library = client_handshake(client, 'secret')
    client_dispatcher.add_worker(None, 7)
    client_library.add('Yellow')
    eventlet.sleep()


    assert dispatcher.add_worker.called
    library.add.assert_called_with('Yellow')

def test_server_handshake_unauthorized():
    server, client = create_pipes()
    dispatcher = Mock(Dispatcher)
    library = Mock(ConfigurationLibrary)
    handler = ConnectionHandler(dispatcher, library, 'secret')

    thread = eventlet.spawn( handler.new_connection, server )

    assert_raises(ConnectionLost, client_handshake, client, 'seret')




def basic_web_handler(env, start_response):
    start_response('200 OK', [('Content-Type', 'text/plain')])
    return ["Hello World"]

class FakeConnectionHandler(object):
    def __init__(self):
        self.incoming = None

    def new_connection(self, connection):
        connection.write('HELLO')
        connection.flush()
        connection.close()

def test_web():
    webserver = WebServer(basic_web_handler, None)
    eventlet.spawn( webserver.listen, ('', 1996) )
    eventlet.sleep()

    data = urllib2.urlopen('http://localhost:1996').read()
    assert_equals(data, 'Hello World')

def test_not_web():
    webserver = WebServer(None, FakeConnectionHandler())
    eventlet.spawn( webserver.listen, ('', 1997) )
    eventlet.sleep()

    client = client_web_connect( ('', 1997) )
    data = client.read(5)
    assert_equals(data, 'HELLO')

