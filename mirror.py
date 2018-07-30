import logging
import yaml
from FileStorage import FileStorage
from DixelKit.Orthanc import Orthanc
from DixelKit.DixelStorage import CachePolicy
import time
import sys
import os

def mirror(root, orthanc, start, num_to_do, cache_policy=None):
    count = 0

    dirs = [os.path.join(root, o) for o in os.listdir(root)
                                   if os.path.isdir(os.path.join(root, o))]
    dirs = sorted(dirs)[start[0]:]
    for i, dir in enumerate(dirs, start[0]):
        logging.debug("[{}]: {}".format(i, dir))

        subdirs = [os.path.join(dir, o) for o in os.listdir(dir)
                                              if os.path.isdir(os.path.join(dir, o))]
        subdirs = sorted(subdirs)[start[1]:]

        for j, subdir in enumerate(subdirs, start[1]):
            sp = os.path.join(root, dir, subdir)
            logging.debug("[{},{}]: {}".format(i,j,sp))

            file_dir = FileStorage(sp, cache_policy=cache_policy)
            t0 = time.time()
            copied = file_dir.copy_inventory(orthanc, lazy=True)
            t1 = time.time()

            logging.debug("copied {} dixels in {} seconds".format(copied, t1-t0))

            count = count + 1
            if count >= num_to_do:
                break

        if count >= num_to_do:
            break

import multiprocessing as mp

def multi_mirror(root, orthanc, num_procs=8):

    for i in range(0, num_procs):
        start = [i * 256/num_procs, 0]
        num_to_do = 256*256/num_procs

        num_to_do = 1

        p = mp.Process(target=mirror, args=(root, orthanc, start, num_to_do))
        p.start()


if __name__=="__main__":

    logging.basicConfig(level=logging.DEBUG)
    with open("secrets.yml", 'r') as f:
        secrets = yaml.load(f)

    root = "/users/derek/Desktop/Protect3"
    orthanc = Orthanc('localhost', 8042)
    # orthanc = Orthanc(**secrets['services']['cirr1'])

    try:
        start = [int(sys.argv[1]), int(sys.argv[2])]
        num_to_do = int(sys.argv[3])
    except:
        pass

    try:
        # Useful when Dixel definition changes
        if sys.argv[4] == "CLEAN":
            cache_policy = CachePolicy.CLEAR_AND_USE_CACHE
        else:
            cache_policy = CachePolicy.USE_CACHE
    except:
        pass

    # mirror(root, orthanc, start, num_to_do, cache_policy)

    multi_mirror(root, orthanc, 4)