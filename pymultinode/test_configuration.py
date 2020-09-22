from __future__ import absolute_import
"""
Tests for the configuration module

NOTE: This file is abused by the test code for the testing of configurations
As a result it will be loaded inside another process outside of the context
of being in a package. Whatever is done in here needs to stay working when
that happens
"""
from pymultinode.configuration import extract_configuration, Configuration
from pymultinode.configuration import ConfigurationLibrary, get_python_files
from nose.tools import assert_equal, assert_raises
import zipfile
from StringIO import StringIO
import os.path
import multiprocessing
import contextlib

def quick_config(compiled = False, parent = False):
    """
    Return a configuration

    compiled determines whether we pass the .pyc or .py version of the filename
    if parent is true, we produce a configuration grounded in the parent
    directory
    """

    # throw away the current extension
    base = os.path.splitext(__file__)[0]

    if parent:
        # doing this moves us up a directory
        base = os.path.dirname(base)

    # add an extension
    if compiled:
        path = base + '.pyc'
    else:
        path = base + '.py'

    # build the extension
    return extract_configuration(path)

def quick_config_zip(compiled = False, parent = False):
    """
    Take the quick_config generated configuration and
    return the zip file it contains

    parameters have same meanings as quick_config
    """
    config = quick_config(compiled, parent)

    # convert the string into a zipfile
    contents = StringIO(config.contents)
    return zipfile.ZipFile(contents)

def quick_config_files(compiled = False, parent = False):
    """
    Return the list of files that quick configuration compiled
    """
    return quick_config_zip(compiled, parent).namelist()

def test_configuration():
    """
    We should package the .pyc version of the file
    """
    assert 'test_configuration.pyc' in quick_config_files()

def test_configuration_pyc():
    """
    We should tolerate getting a .pyc as the base script
    """
    assert 'test_configuration.pyc' in quick_config_files(True)

def test_configuration_other_scripts():
    """
    make sure that other scripts in the directory are included
    """
    assert 'configuration.pyc' in quick_config_files()

def test_includes_packages():
    """
    Make sure that any packages get added
    """
    assert 'pymultinode/configuration.pyc' in quick_config_files(parent = True)

def test_has_hash():
    """
    Make sure that generating the configuration twice produces the same hash
    """
    assert_equal( quick_config().hash, quick_config().hash )


def do_within_applied(config, client, function):
    """
    Used by within_applied, this function simply runs function
    within a particular configuration.

    This function will be called in a different process
    """
    with config.apply():
        function()
        # We need to have a way to tell the parent process
        # what happened, so we simply send the value True
        client.send(True)
        client.close()

def within_applied(function):
    """
    The decorator causes the function it is applied to run within the context
    of a subprocess which the quick_config() configuration has been applied
    """
    def inner():
        config = quick_config()

        # we use the pipe to communicate with the subprocess
        server, client = multiprocessing.Pipe()

        # create, start, and wait for the process to run
        process = multiprocessing.Process(target = do_within_applied,
            args = (config,client, function))
        process.start()
        process.join()

        # the process has terminated, if nothing was written to the pipe
        # it failed to reach the end of the function
        assert server.poll()
        # make sure the right thing was written to the pipe
        assert_equal(True, server.recv())

    # make sure the name is correct so that nose will recognize the tests

    inner.__name__ = 'test_' + function.__name__


    return inner


def do_apply_configuration_import():
    """
    configuration should be importable now since it was included in the package
    """
    __import__('configuration')
test_apply_configuration_import = within_applied(do_apply_configuration_import)


def do_main_is_correct():
    """
    __main__ should refer to this test_configuration scripts.
    """
    __import__('__main__').test_main_is_correct # check to see whether this test is there
test_main_is_correct = within_applied(do_main_is_correct)

def configuration_a():
    "Return a simple configuration: 'A'"
    return Configuration('A', 'A')

def configuration_b():
    "Return a simple configuration: 'B'"
    return Configuration('B', 'B')

def configuration_c():
    "Return a simple configuration: 'C'"
    return Configuration('C', 'C')

def configuration_library():
    """
    Construction a configuration library containing A and B but not C
    """
    library = ConfigurationLibrary()
    library.add( configuration_a() )
    library.add( configuration_b() )
    return library

def test_library():
    """
    I should be able to get at the configuration I have stored
    """
    library = configuration_library()
    config = configuration_a()
    assert_equal( config.contents, library.get(config.hash).contents )

def test_missing():
    """
    Accessing missing configurations should raise a KeyError
    """
    library = configuration_library()
    config = configuration_c()
    assert_raises(KeyError, library.get, config.hash)

def test_library_remove():
    """
    Make sure that I can remove configurations
    """
    library = configuration_library()
    config = configuration_a()
    library.remove(config.hash)
    assert_raises(KeyError, library.get, config.hash)

def test_library_remove_missing():
    """
    Removing a configuration that is not there should raise a KeyError
    """
    library = configuration_library()
    config = configuration_c()
    assert_raises(KeyError, library.remove, config.hash)

class NullConfiguration(object):
    """
    NullConfiguration follows the interface of Configuration but does not
    actually do anything.


    It also has an applied attribute which is True while it is applied
    """
    def __init__(self, hash):
        self.applied = False
        self.hash = hash

    @contextlib.contextmanager
    def apply(self):
        self.applied = True
        yield
        self.applied = False


def test_null_configuration():
    """
    Make sure that NullConfiguration follows the correct rules about when it is
    applied
    """
    null_config = NullConfiguration(None)
    assert_equal(null_config.applied, False )
    null_config.apply()
    assert_equal(null_config.applied, False )
    with null_config.apply():
        assert_equal(null_config.applied, True )
    assert_equal(null_config.applied, False )


def test_null_configuration_hash():
    """
    Make sure that null_configuration has a hash
    """
    null_config = NullConfiguration('NULL-HASH')
    assert_equal(null_config.hash, 'NULL-HASH')

def test_empty_path():
    """
    Make sure that we can locate files in empty path
    """
    list(get_python_files(''))

