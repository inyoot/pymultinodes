from .protocol import CommandCodes
from .serialization import dumps
from pickle import loads
import eventlet

class ConfigurationLibraryServer(object):
    """
    Connect an instance of Configuration library to a protocol
    so that it can be controlled by client
    """
    def __init__(self, library, protocol):
        self._library = library
        self._protocol = protocol
        
        protocol.register_handler( CommandCodes.AddConfiguration, self.add )
        protocol.register_handler( CommandCodes.GetConfiguration, self.get )
        protocol.register_handler( CommandCodes.RemoveConfiguration, self.remove )

    def add(self, command, sequence, data):
        self._library.add( loads(data) )

    def get(self, command, sequence, data):
        result = self._library.get(data)
        encoded_result = dumps(result)
        self._protocol.respond(sequence, encoded_result)

    def remove(self, command, sequence, data):
        self._library.remove(data)





class ConfigurationLibraryClient(object):
    """
    A proxy for a ConfigurationLibrary on the other side of a socket
    """
    def __init__(self, protocol):
        self._protocol = protocol

    def add(self, configuration):
        self._protocol.command( CommandCodes.AddConfiguration, dumps(configuration) )

    def get(self, configuration_id):
        event = self._protocol.request( CommandCodes.GetConfiguration, configuration_id)
        result = event.wait()
        return loads(result)

    def remove(self, configuration_id):
        self._protocol.command( CommandCodes.RemoveConfiguration, configuration_id )

class WorkerServer(object):
    """
    Proxy that gives commands to a worker
    """
    def __init__(self, worker, protocol):
        self._worker = worker
        self._protocol = protocol

        protocol.register_handler( CommandCodes.WorkerTask, self.do_task)

    def do_task(self, command, sequence, data):
        configuration_id, task = loads(data)
        def inner():
            try:
                print "START TASK"
                result = self._worker.do_task(configuration_id, task)
                print "FINISH TASK"
                self._protocol.respond(sequence, result)
            except:
                print "OK?"
                self._protocol.close()
                raise

        eventlet.spawn_n(inner)

class WorkerClient(object):
    """
    Proxy to send worker commands across the protocol
    """
    def __init__(self, protocol):
        self._protocol = protocol

    def do_task(self, configuration_id, task):
        event = self._protocol.request( CommandCodes.WorkerTask, dumps( (configuration_id, task)) )
        return event.wait()

    def data(self):
        data = {
            'type' : 'remote'
        }
        return data

class DispatcherServer(object):
    """
    Proxy to send request to dispatcher
    """
    def __init__(self, dispatcher, protocol):
        self._dispatcher = dispatcher
        self._protocol = protocol

        self._protocol.register_handler( CommandCodes.AddWorker, self.add_worker )
        self._protocol.register_handler( CommandCodes.DispatchTask, self.do_task )

    def add_worker(self, command, sequence, data):
        times = loads(data)
        worker = WorkerClient(self._protocol)
        self._dispatcher.add_worker(worker, times)

    def do_task(self, command, sequence, data):
        configuration_id, task = loads(data)

        # create a green thread which waits for the response
        # and then sends it off
        def inner():
            result = event.wait()
            self._protocol.respond(sequence, result)

        event = self._dispatcher.do_task(configuration_id, task)
        eventlet.spawn_n(inner)



class DispatcherClient(object):
    """
    Proxy for the dispatcher using a protocol
    """
    def __init__(self, protocol):
        self._protocol = protocol

    def add_worker(self, worker, times):
        server_worker = WorkerServer(worker, self._protocol)
        self._protocol.command( CommandCodes.AddWorker, dumps(times) )

    def do_task(self, configuration_id, task):
        return self._protocol.request( CommandCodes.DispatchTask, dumps( (configuration_id, task) ) )
