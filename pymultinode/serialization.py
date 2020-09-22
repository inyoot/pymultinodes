from multiprocessing.forking import ForkingPickler
from StringIO import StringIO

def dump(obj, file, protocol = 2):
    """
    Serialize obj to file using the enhancments made in multiprocessing
    """
    return ForkingPickler(file, protocol).dump(obj)

def dumps(obj, protocol = 2):
    """
    Serialize obj to string using the enhancments made in multiprocessing
    """
    output = StringIO()
    ForkingPickler(output, protocol).dump(obj)
    return output.getvalue()
