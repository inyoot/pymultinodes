from .handshake import standard_connect
from .configuration import default_configuration
from .processor import Processor

def JobProcessor( secret, address, port = 12456):
    dispatcher, library = standard_connect( (address, port), secret )
    config = default_configuration()
    library.add(config)
    processor = Processor(config.hash, dispatcher)
    return processor

