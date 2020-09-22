"""
pymultinode.worker

These classes take care of actually doing whatever it is the user is requesting
They work by launching a subprocess and doing it there.
"""
import pickle
import sys
from eventlet.green import subprocess
import traceback
from StringIO import StringIO
from .serialization import dumps, dump

# this constant controls the version of the pickle protocol being used
PICKLE_PROTOCOL = 2

import struct

def prepare_pipe(file):
    if sys.platform == 'win32':
        import msvcrt, os
        msvcrt.setmode(file.fileno(), os.O_BINARY)


def run_with_capture(input, output, error):
    """
    This is the entry point for the subprocesses.

    It reads the configuration and task from the standard input as pickled
    objects.
    It writes a tuple back to standard output indicating what happened
    """

    # The standard output and error and used to communicate with the
    # parent process so I have to prevent any code being run from acessing
    # them
    standard_output = sys.stdout
    standard_error = sys.stderr


    # I capture the output so that I can send it over to the user
    sys.stdout = StringIO()
    sys.stderr = StringIO()
    fake_stdout = sys.stdout
    fake_stderr = sys.stderr


    try:
        function = pickle.load(input)
        # the actual task itself is in function
        result = function()
        # report everything that happened back to the parent process
        encode_result(output, True, result, fake_stdout.getvalue(), fake_stderr.getvalue())
    except:
        # if somemething goes wrong...
        # first, stop redirecting the output
        # if somebody writes to stderr, the parent process should see it and complain
        sys.stdout = standard_output
        sys.stderr = standard_error

        # get the exception, and attach the original_traceback to the message
        error_type, error_value, error_tb = sys.exc_info()
        if not hasattr(error_value, 'original_traceback'):
            error_value.original_traceback = traceback.format_exc()

        # report the result back to the parent process
        encode_result(output, False, error_value, fake_stdout.getvalue(), fake_stderr.getvalue())
    else:
        sys.stdout = standard_output
        sys.stderr = standard_error

def subtask():
    """
    This is the entry point for the subprocesses.

    It reads the configuration and task from the standard input as pickled
    objects.
    It writes a tuple back to standard output indicating what happened
    """
    standard_error = sys.stderr
    standard_output = sys.stdout

    prepare_pipe(standard_output)
    prepare_pipe(standard_error)
    prepare_pipe(sys.stdin)

    print >> sys.stderr, "Starting worker process"

    sys.stdout = sys.stderr
    try:
        # the first thing we should read is the configuration object
        # pymultinode.configuration
        print >> standard_error, "Loading configuration"
        configuration = pickle.load(sys.stdin)
        print >> standard_error, "Configuration loaded"
        with configuration.apply():
            standard_output.write('1')
            standard_output.flush()
            command = ProcessCommandCodes.Task

            while command == ProcessCommandCodes.Task:
                command = sys.stdin.read(1)

                if command == ProcessCommandCodes.Task:
                    # the actual task itself is in function
                    run_with_capture(sys.stdin, standard_output, standard_error)
    except:
        standard_output.write('0')
        # get the exception, and attach the original_traceback to the message
        error_type, error_value, error_tb = sys.exc_info()
        if not hasattr(error_value, 'original_traceback'):
            error_value.original_traceback = traceback.format_exc()

        # report the result back to the parent process
        encode_result(standard_output, False, error_value)

class ProcessCommandCodes:
    Quit = 'Q'
    Task = 'T'

LENGTH = struct.Struct('L')

class Worker(object):
    """
    The Worker object takes care of handing to task off to a subprocess
    """
    def __init__(self, library):
        """
        Construct a worker

        library should be a ConfigurationLibrary instance, used to obtain the
        configurations
        """
        self._library = library
        self._processes = []

    def data(self):
        data = {
            'type' : 'local',
            'processes' : len(self._processes)
        }
        return data

    def do_task(self, configuration_id, task):
        """
        Actual method to do the task

        configuration_id should be the hash of the configuration object
        task should be a string which is a pickled callable doing the actual
        job
        """
        if self._processes:
            current_configuration_id, process = self._processes.pop()
            if current_configuration_id != configuration_id:
                process.stdin.write( ProcessCommandCodes.Quit )
                process.wait()
                process = None
        else:
            process = None


        if process is None:
            print "Starting new slave"
            configuration = self._library.get(configuration_id)

            # load a python process that import this module and calls subtask
            program = 'import pymultinode.worker;pymultinode.worker.subtask()'

            # if sys.stderr is a real file, just hook the client stderr to it
            # else connect it to a pipe which we ignore
            if hasattr(sys.stderr, 'fileno'):
                error = sys.stderr
            else:
                error = subprocess.PIPE

            process = subprocess.Popen([sys.executable, '-c', program], executable = sys.executable,
                    stdout = subprocess.PIPE, stdin = subprocess.PIPE,
                    stderr = error)
            prepare_pipe(process.stdin)
            prepare_pipe(process.stdout)

            # write configuration on the processes stdin
            print "Sending slave configuration data"
            dump(configuration, process.stdin)
            process.stdin.flush()
            print "Awaiting slave response"
            response = process.stdout.read(1)
            if response == '0':
                print "Slave failed to configure"
                length = LENGTH.unpack( process.stdout.read(LENGTH.size) )[0]
                output = process.stdout.read(length)
                process.wait()
                return output


        print "Submitting slave task"
        process.stdin.write( ProcessCommandCodes.Task )
        process.stdin.write(task)
        process.stdin.flush()
        print "Waiting for slave response"
        length = LENGTH.unpack( process.stdout.read(LENGTH.size) )[0]
        output = process.stdout.read(length)

        self._processes.append( (configuration_id, process) )
        print "Returning response"
        # the standard output contains the representation of the result
        return output

def encode_result(output, success, value, stdout = '', stderr = ''):
    product = (success, value, stdout, stderr)
    encoded = dumps(product, PICKLE_PROTOCOL)
    output.write( LENGTH.pack(len(encoded)) )
    output.write(encoded)
    output.flush()

def evaluate_result(pickled_result):
    """
    Take the result returned from Worker.do_task and interpret it
    """
    # its a pickled object with four parts
    code, result, stdout, stderr = pickle.loads(pickled_result)
    # just dump the stderr and stdout
    sys.stderr.write(stderr)
    sys.stdout.write(stdout)
    # code indicates whether we had an exception or not
    if code:
        return result
    else:
        raise result
