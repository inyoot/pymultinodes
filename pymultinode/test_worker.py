"""
Tests for pymultinode.worker
"""
from .worker import Worker, evaluate_result
from .configuration import ConfigurationLibrary, single_file_configuration
from .test_configuration import NullConfiguration
import pickle
from nose.tools import assert_equals, assert_raises
from nose import SkipTest
import sys
from StringIO import StringIO

def return_42():
    """
    A test function that just returns 42
    """
    return 42

def quick_worker():
    """
    Returns a worker configured to have a null configuration available on
    hash code 'nothing'
    """
    library = ConfigurationLibrary()
    worker = Worker(library)
    library.add( NullConfiguration('nothing') )
    return worker

def quick_task(function):
    """
    Run the given task on a worker and return the result
    """
    worker = quick_worker()
    result = worker.do_task( 'nothing', pickle.dumps(function) )
    return evaluate_result(result)

def test_return_42_task():
    """
    Make sure we run a simple function
    """
    assert_equals( 42, quick_task(return_42) )

def vandalizes():
    """
    Makes a modification to sys
    """
    sys.vandalized = True

def test_worker_is_isolated():
    """
    Make sure any modification made to the system stay in the subprocess
    """
    quick_task(vandalizes)
    assert not hasattr(sys, 'vandalized')

def errant():
    """
    Raises an error
    """
    raise IndexError("You failed")

def test_errant_is_propagated():
    """
    Make sure that errors get propagated
    """
    assert_raises( IndexError, quick_task, errant)

def printing():
    """
    Prints a greeting
    """
    print 'Hello World'

def test_printing_works():
    """
    Make sure that printing in the subprocess ends up being printed to local
    """
    old_stdout = sys.stdout
    try:
        sys.stdout = StringIO()
        quick_task(printing)
        assert_equals( 'Hello World\n', sys.stdout.getvalue())
    finally:
        sys.stdout = old_stdout

def printing_error():
    """
    Prints a warning to stderr
    """
    sys.stderr.write('PANIC!')

def test_printing_works_stderr():
    """
    Make sure that printing to stderr ends up getting printed to parent stderr
    """
    old_stderr = sys.stderr
    try:
        sys.stderr = StringIO()
        quick_task(printing_error)
        assert_equals( 'PANIC!', sys.stderr.getvalue())
    finally:
        sys.stderr = old_stderr

def test_invalid_pickle():
    """
    Make sure nothing explodes when the pickle is invalid
    """
    raise SkipTest()
    # I don't see a way to tell the difference  between a process waiting for more pickle data
    # and one that runs a long time
    worker = quick_worker()
    result = worker.do_task( 'nothing', 'garbage' )

def unpicklable():
    """
    Return something that cannot be pickled
    """
    try:
        raise RuntimeError('oops')
    except:
        # this gets a traceback which cannot be pickled
        return sys.exc_info()[-1]

def test_unpicklable():
    """
    Make sure that failing to pickle the result gives a PicklingError
    """
    assert_raises(pickle.PicklingError, quick_task, unpicklable)

PICKLED_MAIN_CALC = 'c__main__\ncalc\np0\n.'

def test_code_being_sent():
    """
    The code I sent should be accesible by the function
    """
    config = single_file_configuration('alfred', 'def calc(): return 8')
    library = ConfigurationLibrary()
    library.add(config)
    worker = Worker(library)
    result = worker.do_task(config.hash, PICKLED_MAIN_CALC)
    assert_equals( 8, evaluate_result(result) )

def problem_pickler():
    """
    Returns a partial function
    """
    import functools
    import math
    return functools.partial(math.log, 2, 2)

def test_pickle_partial():
    """
    Make sure that we can return a partial function
    partial cannot be pickled by the standard python pickler
    """
    assert_equals( 1, quick_task(problem_pickler)() )

def problem_error():
    """
    Raises an exception with a partial connected
    """
    import functools
    import math
    raise IndexError( functools.partial(math.log, 2, 2) )

def test_problem_error():
    """
    Make sure that partials connected to errors get pickled
    """
    assert_raises(IndexError, quick_task, problem_error)

def test_configuration_pickle():
    """
    Attach a partial to the configuration object and make sure it works
    """
    import functools
    config = NullConfiguration('alfred')
    config.silly = functools.partial(NullConfiguration)

    library = ConfigurationLibrary()
    library.add(config)
    worker = Worker(library)
    result = worker.do_task(config.hash, pickle.dumps(return_42) )
    evaluate_result(result)

def test_configuration_error():
    """
    Make sure that errors gotten while loading configuratin work
    """
    config = single_file_configuration('fred', 'raise IndexError()')
    library = ConfigurationLibrary()
    library.add(config)
    worker = Worker(library)
    result = worker.do_task(config.hash, pickle.dumps(return_42) )
    assert_raises( IndexError, evaluate_result, result)
