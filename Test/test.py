import sys
sys.path[:] = [path for path in sys.path if path.find('pymultinode') == -1]
sys.path.append('.')
from pymultinode import JobProcessor
import time

import numpy as np

def search(w):
    return np.sum(w)

def main():
    
    s_pos = np.zeros((100,6))
    
    for i in np.arange(100):
        s_pos[i] = 2 * np.random.rand(6) - 1
        #s_pos[i] = f_makeitvalid(s_pos[i])
    
    processor = JobProcessor( 'superres', '129.62.150.188' )
    for i, result in enumerate( processor.imap(search, s_pos) ):
        print i, result
        print s_pos[i]
    toc = time.clock();

if __name__ == '__main__':
    main()
    
