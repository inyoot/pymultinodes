import sys
sys.path[:] = [path for path in sys.path if path.find('pymultinode') == -1]
sys.path.append('.')
from pymultinode import JobProcessor
import time

import numpy as np

def search(number):
    #De Jong's function
    value = number[0] * number [0] + number[1] * number[1]
    f = open('workfile', 'w')
    f.write('I can write  \n')
    f.close()
    return value

def main():
    minimum_value = np.finfo(float).max

    processor = JobProcessor( 'secret', 'MRES07.ecs.baylor.edu' )
    myrange = [(x,y) for x in np.arange(-1,1,.01) for y in np.arange(-1,1,.01)]
    print len(myrange)
    for number, result in enumerate( processor.imap(search, myrange) ):
        if minimum_value > result :
            print "({:2.4f},{:2.4f})".format(*myrange[number])
            print"{:2.6f}".format(result)
            minimum_value = result

if __name__ == '__main__':
    main()
