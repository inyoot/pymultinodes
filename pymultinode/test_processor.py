"""
Tests for the processor
"""
import eventlet
import pickle
from .worker import run_with_capture, evaluate_result
from .processor import Processor
from nose.tools import assert_equals, assert_raises
import math
from StringIO import StringIO
import sys
CONFIG_ID = 'JHERICO'

def quick_task(task):
    """
    Does the same thing that worker does, but will less overhead
    """
    input = StringIO(task)
    output = StringIO()
    run_with_capture(input, output, sys.stderr)
    return output.getvalue()[4:]

class FakeDispatcher(object):
    """
    Supports the same interface as the dispatcher, but avoids the
    overhead, and forces the correct configuration_id
    """
    def do_task(self, configuration_id, task):
        assert CONFIG_ID == configuration_id
        return eventlet.spawn( quick_task, task )


def quick_processor():
    """
    Construct a processor useful for testing
    """
    processor = Processor(CONFIG_ID, FakeDispatcher() )
    return processor

def assert_job_result(result, *args, **kwargs):
    """
    Submit a request and check the result
    """
    event = quick_processor().request(*args, **kwargs)
    assert_equals(result, event.wait() )

def returns_42():
    """
    Just a simple function
    """
    return 42

def test_quick_job():
    """
    Make sure that that function returns what it should
    """
    assert_job_result( 42, returns_42)

def test_arguments():
    """
    Make sure that I can pass arguments
    """
    assert_job_result( 9, sum, [4,5])

def log(x, base):
    """
    Call math.log, exists beacuse the version in the standard library
    does not accept keyword arguments
    """
    return math.log(x, base)

def test_kwargs():
    """
    Test whether I can call with keyword arguments
    """
    assert_job_result(3, log, base = 2, x = 8)

def test_imap():
    """
    Test the imap function
    """
    processor = quick_processor()
    for idx, element in enumerate(processor.imap(math.log, xrange(1, 200))):
        assert_equals( math.log(idx+1), element)

def complain():
    """
    Raise an error
    """
    raise IndexError()

def test_error():
    """
    Make sure that I see the errors raised
    """
    processor = quick_processor()
    event = processor.request(complain)
    assert_raises(IndexError, event.wait)

def test_repeat():
    """
    Use the repeat function
    """
    processor = quick_processor()
    for element in processor.repeat(100, returns_42):
        assert_equals(element, 42)
