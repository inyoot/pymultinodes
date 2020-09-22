"""
pymultinode.configuration

The configuration is the code which is being sent over the wire to be run by
the worker computers. The standard_configuration() function will attempt to
create a configuration by taking everything the __main__ module has access to
"""
import zipfile
from StringIO import StringIO
import os.path
import hashlib
import contextlib
import tempfile
import sys


class Configuration(object):
    """
    The configuration object holds the contents of the code. In particular the
    contents attribute is a string holding a zip file of the all of the python
    scripts included

    There is also a hash attribute which is the hash of the contents

    The attribute name refers to the name of the module that should be __main__
    """
    def __init__(self, contents, name):
        self.contents = contents
        self.name = name

        hasher = hashlib.sha256()
        hasher.update(self.contents)
        self.hash = hasher.digest()

    @contextlib.contextmanager
    def apply(self):
        """
        Return a context manager which will make the contents accessible

        When the context manager closes it will delete the file.
        However, it does nothing to unload the modules from memory.
        """
        
        # in order to be able to import from it, we need a .zip file
        temporary_library = tempfile.NamedTemporaryFile(
                suffix = '.zip', delete = False)

        # dump our contents into that file
        with temporary_library.file as library:
            library.write(self.contents)

        try:
            # this makes the contents of the library accesible
            sys.path.append(temporary_library.name)
            # the __main__ here will be different then the orignal
            # but the user may have pickled objects that refer to stuff in __main__
            # so we do this quick hack to switch it over
            sys.modules['__main__'] = __import__(self.name)
            yield # the code inside the with block runs here
            # I make no attempt to remove what I have done
            # my previous attempts to do that ended poorly
        finally:
            # whatever happens, we should get rid of the file
            os.remove(temporary_library.name)


def get_python_files(base_directory):
    """
    Given base_directory return a generator which is all of the python scripts
    and package within that base_directory
    """
    base_directory = os.path.abspath(base_directory)
    for filename in os.listdir(base_directory):
        path = os.path.join(base_directory, filename)
            
        # we want to identify python scripts
        # we do that by looking for .py 
        if filename.endswith('.py'):
            yield path 
        # if a directory contains __init__.py its a package
        elif os.path.isdir(path):
            if os.path.exists( os.path.join(path, '__init__.py') ):
                yield path

def construct_zip_file(base_directory):
    """
    Given a base_directory return a string of a zip file containing all of
    the python files
    """
    output = StringIO()
    pyzip = zipfile.PyZipFile(output, 'w')

    for python_file in get_python_files(base_directory):
        pyzip.writepy(python_file)

    pyzip.close()
    return output.getvalue()

def module_name(filepath):
    """
    Converts a script file path into a module name

    /home/alfred/go.py -> go
    """
    return os.path.splitext(os.path.basename(filepath))[0]

def extract_configuration(main_python_file):
    """
    Given the file which should be __main__, construct a configuration for it
    """
    base_directory = os.path.dirname(main_python_file)
    contents = construct_zip_file(base_directory)
    return Configuration(contents, module_name(main_python_file))

def default_configuration():
    """
    Build a Configuration based on the current __main__ module
    """
    import __main__
    return extract_configuration(__main__.__file__)

class ConfigurationLibrary(object):
    """
    The ConfigurationLibrary keeps track of a number of configurations
    they are tracked by there hash.

    The only thing that this object assumes is the prescence of the hash
    attribute.
    """
    def __init__(self):
        self._configurations = {}

    def add(self, configuration):
        """
        Add a configuration to the hash
        """
        self._configurations[configuration.hash] = configuration

    def get(self, hash):
        """
        Return the configuration corresponding to the hash
        """
        return self._configurations[hash]

    def remove(self, hash):
        """
        Remove the configuration corresponding to the hash
        """
        del self._configurations[hash]

def single_file_configuration(name, contents):
    output = StringIO()
    zipped = zipfile.ZipFile(output, 'w')
    zipped.writestr(name + '.py', contents)
    zipped.close()
    return Configuration( output.getvalue(), name)

