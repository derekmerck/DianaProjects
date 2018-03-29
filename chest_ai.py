"""
Chest AI Pathology Cohort
Merck, Winter 2018

Status: In-progress (pulling)

~17k subjects with RADCAT'd P/A chest CR, 2017-2018

- Load Montage spreadsheets and label by radcat
- Lookup UIDS and other patient data from PACS
- Assign anonymized id, name, dob
- Build out final metadata
- Retrieve, anonymize, download, save

Uses DianaFuture

TODO: IDENTIFY LATS VS APS
"""

import logging, os, re, yaml
from DianaFuture import CSVCache, RedisCache, Dixel, DLVL, Orthanc, \
    lookup_uids, set_anon_ids, copy_from_pacs, create_key_csv

# ---------------------------------
# CONFIG
# ---------------------------------

data_root = "/Users/derek/Projects/Body/XR Chest AI/data"
save_dir = "/Volumes/3dlab/chest_ai/anon"

# All Montage input
fns = ['chestai-1s.csv', 'chestai-4s.csv']

# Output key file
key_fn = "chestai-17k.csv"

# Local RedisCache project db
db = 12

# proxy service
proxy_svc = "deathstar"
remote_aet = "gepacs"

# Sections to run
INIT_CACHE           = False
LOOKUP_UIDS          = False
CREATE_ANON_IDS      = False
CREATE_KEY_CSV       = False
COPY_FROM_PACS       = True


# ---------------------------------
# SCRIPT
# ---------------------------------

logging.basicConfig(level=logging.DEBUG)

with open("secrets.yml", 'r') as f:
    secrets = yaml.load(f)

R = RedisCache(db=db, clear=INIT_CACHE)

proxy = None

# Option to init by reading key?
if INIT_CACHE:
    # Merge positive and negative spreadsheets and indicate status
    for fn in fns:
        fp = os.path.join(data_root, fn)
        radcat = re.findall("chestai-(\d)s\.csv", fn)[0]  # set radcat from fn
        # logging.debug("radcat: {}".format(radcat))
        M = CSVCache(fp, key_field="Accession Number")
        # Should series level dixels bc we only want the AP/PAs, however
        # they are named inconsistently, so have to pull the whole study
        for k, v in M.cache.iteritems():
            v['radcat'] = radcat
            # v['SeriesDescription'] = "CHEST AP\PA"  # Query string for later
            d = Dixel(key=k, data=v, cache=R, remap_fn=Dixel.remap_montage_keys, dlvl=DLVL.STUDIES)

# This takes ~15 mins
if LOOKUP_UIDS:
    proxy = Orthanc(**secrets['services'][proxy_svc])
    lookup_uids(R, proxy, remote_aet, lazy=True)

if CREATE_ANON_IDS:
    set_anon_ids(R, lazy=True)

if CREATE_KEY_CSV:
    create_key_csv(R, os.path.join(data_root, key_fn))

if COPY_FROM_PACS:
    # TODO: Could identify the AP/PA here, after we have tags
    if not proxy:
        proxy = Orthanc(**secrets['services'][proxy_svc])
    copy_from_pacs(proxy, remote_aet, R, save_dir, depth=2)


