"""
The Processor is the part of the system which the user will directly interact
with. It provides an interface to make requests which will be sent off
"""
from serialization import dumps
import functools
import eventlet
from .worker import evaluate_result

def decode_response(event):
    description = event.wait()
    return evaluate_result(description)

class Processor(object):
    """
    The processor connects to a dispatcher and uses it to perform tasks
    """
    def __init__(self, configuration_id, dispatcher):
        """
        The configuration_id should be the hash of a configuration
        which is properly registered. The dispatcher should be a Dispatcher
        object
        """
        self._configuration_id = configuration_id
        self._dispatcher = dispatcher

        # we keep a pool of green threads around
        self._pool = eventlet.GreenPool()

    def request(self, task, *args, **kwargs):
        """
        Basic api, send a request
        task is the function, whereas args and kwargs are the arguments

        returns a eventlet event function, call .wait() on this to obtain
        the result or raise the exception
        """
        # I can't partial without arguments, so I protect it here
        if args or kwargs:
            task = functools.partial(task, *args, **kwargs)

        # everything is transfered as a string from here to worker
        task_description = dumps(task)

        # the dispatcher does the actual interesting work
        dispatcher_event = self._dispatcher.do_task(self._configuration_id, task_description)
        return eventlet.spawn(decode_response, dispatcher_event)

    def imap(self, task, *iterables):
        """
        Iterate in parallel (ala zip) over all the iterables
        dispatchting calling task on them
        """
        def spawner(*args):
            k = self.request(task, *args)
            r = k.wait()
            return r

        return self._pool.imap(spawner, *iterables)

    def repeat(self, times, *args, **kwargs):
        """
        Repeat the request many times returning an interator with the result

        Intended to be used for monte carlo
        """
        def spawner(idx):
            return self.request(*args, **kwargs).wait()

        return self._pool.imap(spawner, xrange(times))

