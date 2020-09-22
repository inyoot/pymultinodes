"""
Tests for the dispatcher code
"""
from .dispatcher import Dispatcher
from .protocol import ConnectionLost
from nose.tools import assert_equals

class DoubleWorker(object):
    """
    This object implements the worker interface, but just concatenates the task
    with itself
    """
    def __init__(self):
        self.calls = 0

    def do_task(self, configuration_id, task):
        """
        Do a silly implementation of a task
        """
        self.calls += 1
        return task + task

class GiveUpWorker(object):
    """
    Implements the Worker interface by giving up every time
    """

    def do_task(self, configuration_id, task):
        """
        This raises ConnectionLost which indicates that the worker cannot 
        perform tasks for some reason
        """
        raise ConnectionLost()

def test_dispatcher():
    """
    Make sure that the tasks do get done
    """
    dispatcher = Dispatcher()
    dispatcher.add_worker(DoubleWorker(), 1)
    event = dispatcher.do_task(None, 'yellow')
    assert_equals( 'yellowyellow', event.wait() )

def test_dispatcher_early():
    """
    Make sure that requesting a task before adding a worker is ok
    """
    dispatcher = Dispatcher()
    event = dispatcher.do_task(None, 'yellow')
    dispatcher.add_worker(DoubleWorker(), 1)
    assert_equals( 'yellowyellow', event.wait() )

def test_two_workers():
    """
    Ensure that a worker requesting double tasks gets them
    """
    dispatcher = Dispatcher()
    worker = DoubleWorker()
    dispatcher.add_worker(worker, 2)
    dispatcher.add_worker(DoubleWorker(), 2)
    for x in range(2):
        dispatcher.do_task(None, '').wait()

    assert_equals( worker.calls, 2)


def test_repeat_workers():
    """
    Make sure that workers are recycled once they are finished
    """
    dispatcher = Dispatcher()
    worker = DoubleWorker()
    dispatcher.add_worker(worker, 1)
    for x in range(10):
        dispatcher.do_task(None, '').wait()

def test_disabled_workers():
    """
    Make sure that workers who refused to work don't stop us
    """
    dispatcher = Dispatcher()
    worker = DoubleWorker()
    dispatcher.add_worker(worker, 1)
    dispatcher.add_worker(GiveUpWorker(), 1)
    for x in range(10):
        assert_equals('', dispatcher.do_task(None, '').wait() )

def test_disabled_workers_multiple():
    """
    Make sure a double active worker who refuses to work doesn't cause a problem
    """
    dispatcher = Dispatcher()
    worker = DoubleWorker()
    dispatcher.add_worker(worker, 1)
    dispatcher.add_worker(GiveUpWorker(), 2)
    tasks = [dispatcher.do_task(None, '') for x in range(10)]
    for task in tasks:
        assert_equals('', task.wait() )
