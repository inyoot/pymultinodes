import sys
sys.path.append('..')

import pymultinode

def square(x,y):
    return sum(range(x,y))#*sum(range(x,y))

if __name__ == '__main__':
    processor = pymultinode.standard_processor('dd06.ecs.baylor.edu')
    for value in processor.imap(square, xrange(1000), xrange(2000, 3000)):
        print value

