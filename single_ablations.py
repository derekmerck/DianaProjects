"""
Perseon Single-Stick Cohort
Merck, Spring 2018

Status: In-dev

43 subjects with 3D intraop workups

- Load AccessionNumbers
- Lookup UIDS and other patient data from PACS
- Retrieve, push to Hounsfield

Uses DianaFuture
"""

import logging, os, re, yaml
from DianaFuture import CSVCache, RedisCache, Dixel, DLVL, Orthanc, \
    lookup_uids, set_anon_ids, copy_from_pacs, create_key_csv

# ---------------------------------
# CONFIG
# ---------------------------------

data_root = "/Users/derek/Projects/Ablation Planning/data"
save_dir = "/Volumes/3dlab/ablation/perseon_anon"

# All Montage input
fn = 'perseon_singles.csv'

# Local RedisCache project db
db = 10

# proxy service
proxy_svc = "deathstar"
remote_aet = "gepacs"

# dest service
dest_svc = "hounsfield+ablation"

# Sections to run
INIT_CACHE           = False
LOOKUP_UIDS          = False
COPY_FROM_PACS       = False


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
    proxy = Orthanc(**secrets['services'][proxy_svc])
    lookup_uids(R, proxy, remote_aet, lazy=True)

if COPY_FROM_PACS:
    # TODO: Could identify the AP/PA here, after we have tags
    if not proxy:
        proxy = Orthanc(**secrets['services'][proxy_svc])
    dest = Orthanc(**secrets['services'][dest_svc])
    copy_from_pacs(proxy, remote_aet, R, dest)

