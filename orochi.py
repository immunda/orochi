from poller import Poller
from aggregator import Aggregator
from controller import Controller
from database import Database
import sys

def selection():
    print '\nOrochi Cloud monitoring'
    print '----------------------------\n'
    print 'Please choose which type of node to add:\n'
    print '[c]ontroller, [a]ggregator or [p]oller\n'
    print '>>>',
    choice = raw_input()
    check_node_choice(choice)
    
def seg_selection():
    db = Database()
    segments = db.get_segments()
    print '----------------------------\n'
    print 'Select a segment\n'
    for i in range(len(segments)):
        print '[%s] %s - %s' % ((i+1), segments[i].name, segments[i].description)
    print '\n>>>',
    seg_choice = raw_input()
    try:
        if int(seg_choice) == 0:
            exit()
        segment = segments[int(seg_choice)-1]
        poller = Poller(segment.name)
    except IndexError:
        exit()
    except ValueError:
        exit()

def check_node_choice(node):
    if node == 'c':
        print 'Initializing Controller'
        controller = Controller()
    elif node == 'a':
        print 'Initializing Aggregator'
        aggregator = Aggregator()
    elif node == 'p':
        print '----------------------------\n'
        print 'Would you like to assign the Poller to a specific network segment?\n'
        print '[y]es or [n]o\n'
        print '>>>',
        want_seg = raw_input()
        if want_seg == 'y':
            seg_selection()
        elif want_seg == 'n':
            print 'Initializing Poller'
            poller = Poller()
        else:
            exit()
    else:
        exit()
        
def exit():
    print '\nInvalid choice!'
    sys.exit()

try:
    selection()
except KeyboardInterrupt:
    print '\n\nExiting.'
    sys.exit()