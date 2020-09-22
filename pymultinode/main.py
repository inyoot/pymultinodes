import optparse
from pymultinode import process

def lowpriority():
    """ Set the priority of the process to below-normal."""
    # from http://stackoverflow.com/questions/1023038/change-process-priority-in-python-cross-platform
    import sys
    try:
        sys.getwindowsversion()
    except:
        isWindows = False
    else:
        isWindows = True

    if isWindows:
        #   "Recipe 496767: Set Process Priority In Windows" on ActiveState
        #   http://code.activestate.com/recipes/496767/
        #   Rewritten to use ctypes
        import ctypes
        kernel32 = ctypes.windll.Kernel32
        pid = kernel32.GetCurrentProcessId()
        PROCESS_ALL_ACCESS = (0x000F0000 | 0x00100000 | 0xFFF)
        handle = kernel32.OpenProcess(PROCESS_ALL_ACCESS, False, pid)
        kernel32.SetPriorityClass(handle, 64)
    else:
        import os

        os.nice(1)


USAGE = 'Usage: %prog [options] worker OR dispatcher'
def main():
    parser = optparse.OptionParser(usage = USAGE)
    parser.add_option('-p', '--port', dest='port', default = 12456, help = 'Port number to use', action = 'store', type='int')
    parser.add_option('-s', '--server', dest='server', default = '', help = 'Server address', action = 'store', type='string')
    options, args = parser.parse_args()
    if len(args) != 2 or args[0] not in ['worker', 'dispatcher']:
        parser.error('required worker or dispatcher')
    else:
        lowpriority() # try to avoid making the system unresponsive
        if args[0] == 'worker':
            process.worker_process( (options.server, options.port), args[1] )
        else:
            process.server_process( (options.server, options.port), args[1] )


