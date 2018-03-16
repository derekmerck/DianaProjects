"""
Chest AI Pathology Cohort
Merck, Winter 2018

Uses DianaFuture

~17k subjects with RADCAT'd P/A chest CR, 2016-2018
"""

import logging
import glob
import dateutil.parser
import os
import re
from pprint import pformat

from DianaFuture import CSVCache, RedisCache, Dixel, Orthanc

DATA_ROOT = "/Users/derek/Projects/Body/XR Chest AI/data"

"""
Merge positive and negative spreadsheets and indicate status
"""

logging.basicConfig(level=logging.DEBUG)

# All Montage input
fns = ['chestai-1s.csv', 'chestai-4s.csv']

INIT_REDIS_CACHE     = True
BUILD_CSV_FROM_REDIS = True

R = RedisCache(db=15, clear=INIT_REDIS_CACHE)


if INIT_REDIS_CACHE:
    # Rebuild the redis cache
    for fn in fns:
        fp = os.path.join(DATA_ROOT, fn)
        radcat = re.findall("chestai-(\d)s\.csv", fn)[0]  # set radcat from fn
        # logging.debug("radcat: {}".format(radcat))
        M = CSVCache(fp, key_field="Accession Number")
        for k, v in M.cache.iteritems():
            v['radcat'] = radcat
            d = Dixel(key=k, data=v, cache=R, remap_fn=Dixel.remap_montage_keys)

# LOOKUP SERUIDS

# CREATE ANON_IDS

# COPY FROM PACS TO DISK

# BUILD REPORT CORPUS

if BUILD_CSV_FROM_REDIS:
    N = CSVCache(os.path.join(DATA_ROOT, "chestai-17k.csv"),
                 key_field="AccessionNumber",
                 autosave=False, clear=True)

    for k in R.keys():
        d = Dixel(key=k, cache=R)
        # logging.debug(d.data)
        N.put(k, d.data)

    N.save_fn()
    logging.debug("Saved {} entries".format(len(N)))
