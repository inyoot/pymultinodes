"""
pymultinode.handshake

This module is responsible for the handshake in the protocols.
It allows the connection between the server and client to start
"""
import hashlib
from .protocol import request_protocol_from_file, ConnectionLost
from .proxy import DispatcherClient, ConfigurationLibraryClient
from .proxy import DispatcherServer, ConfigurationLibraryServer
import random
import eventlet.wsgi

CODE_LENGTH = 32
DIGEST_LENGTH = hashlib.sha256().digest_size

class ConnectionHandler(object):
    """
    The ConnectionHandler is setup by the server and is responsible for making
    the connections between the incoming sockets and the accesible interfaces
    """
    def __init__(self, dispatcher, library, secret):
        self._dispatcher = dispatcher
        self._library = library
        self._secret = secret

    def new_connection(self, client):
        """
        Implements the connection handshake
        """
        # we send CODE_LENGTH random challenge
        challenge = ''.join( chr(random.randrange(256)) for x in xrange(CODE_LENGTH) )
        client.write(challenge)
        client.flush()

        # the client will use those challenge and its password to come up with a response
        response = client.read(DIGEST_LENGTH)
        correct = calculate_response(challenge, self._secret)

        if correct == response:
            # tell the cilent we are happy
            client.write('OK')
            client.flush()
            # hook up the objects
            protocol = request_protocol_from_file(client)
            DispatcherServer(self._dispatcher, protocol)
            ConfigurationLibraryServer(self._library, protocol)
            protocol.wait_shutdown()
        else:
            # just dump the client connection
            client.close()


def calculate_response(challenge, secret):
    """
    Given the challenge and secret password determine the correct response
    """
    hasher = hashlib.sha256()
    hasher.update(challenge)
    hasher.update(secret)
    return hasher.digest()

def client_handshake(server, secret):
    """
    Connect to the server given the secret

    server should be a file like object
    """

    # read the challenge (random bytes)
    challenge = server.read(CODE_LENGTH)
    # send the correct response
    server.write( calculate_response(challenge, secret) )
    server.flush()
    # did the server approve
    response = server.read(2)
    if response == 'OK':
        protocol = request_protocol_from_file(server)
        return DispatcherClient(protocol), ConfigurationLibraryClient(protocol)
    else:
        raise ConnectionLost()

class WebServer(object):
    """
    The webserver listens on a specified port for web requests
    Depending on the type of request, it either sends it to the pymultinode
    protocol or to the web protocol
    """
    def __init__(self, web_handler, connection_handler):
        self._web_handler = web_handler
        self._connection_handler = connection_handler

    def listen(self, address):
        """
        Runs a server on the specified address
        """
        eventlet.wsgi.server( eventlet.listen(address), self._request )

    def _request(self, env, start_response):
        """
        Handles a particular request
        """
        if env['PATH_INFO'] == '/pymultinode-client':
            socket = env['eventlet.input'].get_socket().makefile('rw')
            self._connection_handler.new_connection(socket)
            return eventlet.wsgi.ALREADY_HANDLED
        else:
            return self._web_handler(env, start_response)


def client_web_connect(address):
    """
    Connect to the given addres which should have a WebServer listening on it
    """
    print "L"
    socket = eventlet.connect(address).makefile('rw')
    socket.write('GET /pymultinode-client HTTP/1.0\r\n')
    socket.write('\r\n')
    socket.flush()
    return socket

def standard_connect(address, secret):
    socket = client_web_connect(address)
    return client_handshake(socket, secret)
