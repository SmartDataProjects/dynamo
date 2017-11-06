import sys
import objgraph
from functools import wraps

from common.configuration import common_config

def memory_content(interactive = False):
    typestats = dict(objgraph.typestats())

    while True:
        types = []
        for itype, type_name in enumerate(sorted(typestats.keys())):
            print '%d %s: %d' % (itype, type_name, typestats[type_name])
            types.append(type_name)
            cls = eval(type_name)
            if hasattr(cls, '__len__'):
                for obj in objgraph.by_type(type_name):
                    if len(obj) > 1000:
                        print '   Large object (n=%d) [%s ...]' % (len(obj), str(obj[0]))

        if not interactive:
            break

        print 'Inspect? (0-%d or q):' % itype
        response = sys.stdin.readline().strip()
        if response == 'q':
            break

        try:
            itype = int(response)
            objs = objgraph.by_type(types[itype])
        except:
            print 'Unrecognized input %s' % response
            continue

        while True:
            print 'Item #? (0-%d or q):' % len(objs)
            response = sys.stdin.readline()
            if response == 'q':
                break
    
            try:
                iobj = int(response)
                obj = objs[iobj]
            except:
                print 'Unrecognized input %s' % response
                continue

            while True:
                print 'Expression? (use "obj", e.g., len(obj), or q):'
                response = sys.stdin.readline()
                if response == 'q':
                    break
        
                try:
                    print eval(response)
                except:
                    print 'Unrecognized input %s' % response
                    continue


snapshot = {}
def memory_delta():
    typestats = dict(objgraph.typestats())

    names = set(snapshot.keys()) | set(typestats.keys())
    for name in names:
        try:
            now = typestats[name]
        except:
            now = 0

        try:
            before = snapshot[name]
        except:
            before = 0

        print ' %s: %+d' % (name, (now - before))

    snapshot = typestats

def timer(function):
    @wraps(function)
    def function_timer(*args, **kwargs):
        t0 = time.time()
        result = function(*args, **kwargs)
        t1 = time.time()
        if common_config.debug.time_profile:
            logging.info('Wall-clock time for executing %s: %.1fs', function.func_name, t1 - t0)

        return result

    return function_timer
