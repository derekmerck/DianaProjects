"""
ED Non-contrast Heads (TBI screening) Cohort
Merck, Spring 2018

Montage search
--------------------
IMG181 11/1/17 -> 5/31 (6mos) + RADCAT1 = normal (~1800)
IMG181 11/1/17 -> 5/31 (6mos) + RADCAT4 = priority (has a bleed) (~1600)

- down to 2200, the other 1000 are probalby "mislabelled"

- Not bone window or scouts, just thick brain window series
- Save as DCM stacks in Yes/No folders (RADAT1 vs. RADCAT4)
"""

import logging, yaml, os
from DianaFuture import RedisCache, CSVCache, Orthanc, Dixel, DLVL, lookup_child_uids, create_key_csv, set_anon_ids, copy_from_pacs

# ---------------------------------
# CONFIG
# ---------------------------------

data_root   = "/Users/derek/Desktop/aiim/data"
save_root   = "/Volumes/3dlab/tbi_ai/nc_head_anon"

# Montage input
input_fns = ["search_RADCAT1.csv", "search_RADCAT4.csv"]

# Output key file
key_fn = "tbi_ai.key.csv"

# Local RedisCache project db
db_studies = 8
db_series  = 7

# proxy service config
svc_domain = "lifespan"
proxy_svc  = "deathstar1"
remote_aet = "gepacs"

# Sections to run
INIT_CACHE          = False
LOOKUP_CHILD_UIDS   = False
SET_ANON_IDS        = False
RELOAD_CACHE        = False
COPY_FROM_PACS      = False


# ---------------------------------
# SCRIPT
# ---------------------------------

logging.basicConfig(level=logging.DEBUG)

with open("secrets.yml", 'r') as f:
    secrets = yaml.load(f)
    services = secrets['services'][svc_domain]

R = RedisCache(db=db_studies, clear=(INIT_CACHE or RELOAD_CACHE))
Q = RedisCache(db=db_series,  clear=(INIT_CACHE or RELOAD_CACHE))

proxy = None

if INIT_CACHE:

    for fn in input_fns:
        radcat = fn.find("1") > -1 and 1 or 4  # 1 or 4 in file name
        fp = os.path.join(data_root, fn)
        M = CSVCache(fp, key_field="Accession Number")
        for k, v in M.cache.iteritems():
            v['radcat'] = radcat
            d = Dixel(key=k, data=v, cache=R, remap_fn=Dixel.remap_montage_keys, dlvl=DLVL.STUDIES)


if LOOKUP_CHILD_UIDS:

    proxy = Orthanc(**services[proxy_svc])
    child_qs = [
        { 'SeriesDescription': 'axial*brain reformat' } ]
    lookup_child_uids(R, Q, child_qs, proxy, remote_aet)

    fp = os.path.join(data_root, key_fn)
    create_key_csv(Q, fp, key_field="SeriesInstanceUID")


if SET_ANON_IDS:

    for k,v in Q.cache.iteritems():
        if v['_dlvl'] == 'studies':
            Q.delete(k)

    set_anon_ids(cache=Q)

    fp = os.path.join(data_root, key_fn)
    create_key_csv(Q, fp, key_field="SeriesInstanceUID")


if RELOAD_CACHE:
    fp = os.path.join(data_root, key_fn)
    M = CSVCache(fp, key_field="SeriesInstanceUID")
    for k, v in M.cache.iteritems():
        d = Dixel(key=k, data=v, cache=Q)


if COPY_FROM_PACS:

    # TODO: Add yes/no buckets
    if not proxy:
        proxy = Orthanc(**services[proxy_svc])
    copy_from_pacs(proxy, remote_aet, Q, save_root, depth=1)



