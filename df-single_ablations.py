"""
Perseon Single-Stick Cohort
Merck, Spring 2018

Status: In-dev

43 subjects with 3D intraop workups

- Load AccessionNumbers
- Lookup UIDS and other patient data from PACS
- Set anon ids anonymize
- Retrieve, anon, save zip to disk

Uses DianaFuture
"""

import logging, os, re, yaml, hashlib
from DianaFuture import CSVCache, RedisCache, Dixel, DLVL, Orthanc, \
    lookup_uids, set_anon_ids, copy_from_pacs, create_key_csv

# ---------------------------------
# CONFIG
# ---------------------------------

data_root = "/Users/derek/Projects/Ablation Planning/data"
save_root = "/Volumes/3dlab/ablation/perseon_anon"

# All Montage input
fn = 'perseon_singles.csv'
key_fn = 'perseon_singles.key.csv'

# Local RedisCache project db
db = 10

# proxy service
domain = "lifespan"
proxy_svc = "deathstar2"
remote_aet = "gepacs"

# Sections to run
INIT_CACHE           = False
LOOKUP_UIDS          = False
SET_ANON_IDS         = False
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
    fp = os.path.join(data_root, fn)
    M = CSVCache(fp, key_field="Accession Number")
    for k, v in M.cache.iteritems():
        d = Dixel(key=k, data=v, cache=R, remap_fn=Dixel.remap_montage_keys, dlvl=DLVL.STUDIES)

if LOOKUP_UIDS:
    proxy = Orthanc(**secrets['services'][domain][proxy_svc])
    lookup_uids(R, proxy, remote_aet, lazy=True)

    fp = os.path.join(data_root, key_fn)
    create_key_csv(R, fp, key_field="AccessionNumber")

if SET_ANON_IDS:

    set_anon_ids(cache=R)
    for k, v in R.cache.iteritems():
        d = Dixel(key=k, data=v, cache=R)
        d.data['AnonAccessionNum'] = hashlib.md5(d.data["AccessionNumber"]).hexdigest()
        d.data['status'] = 'ready'
        d.persist()

    fp = os.path.join(data_root, key_fn)
    create_key_csv(R, fp, key_field="AccessionNumber")

if COPY_FROM_PACS:
    if not proxy:
        proxy = Orthanc(**secrets['services'][domain][proxy_svc])
    copy_from_pacs(proxy, remote_aet, R, save_root)

