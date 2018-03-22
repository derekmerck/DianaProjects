"""
Mam MR Biopsy-Proven Cancer Status Cohort
Merck, Winter 2018

550 subjects with breast MR and confirmed cancer status

- Load Montage spreadsheets and label by cancer status
- Lookup UIDS and other patient data from PACS
- Assign anonymized id, name, dob
- Retrieve, anonymize, download, save
- Build out final metadata

Uses DianaFuture
"""

import logging, os, re, yaml
from DianaFuture import CSVCache, RedisCache, Dixel, DLVL, Orthanc, \
    lookup_uids, set_anon_ids, copy_from_pacs, create_key_csv

# ---------------------------------
# CONFIG
# ---------------------------------

data_root = "/Users/derek/Projects/Mammography/MR Breast ML/data"
dest_svc = "Hounsfield+MRBreastML"
save_dir = "/Volumes/3dlab/MRBreast/anon"

# All Montage input
fns = []
# Montage csv dump of 4s and 5s with UNKNOWN status
fns.append("birads_4_5+unk.csv")

# Montage csv dump of 4s and 5s with KNOWN POSITIVE status
# ("BI-RADS CATEGORY 4" | "BI-RADS CATEGORY 5") & ("Recently diagnosed" | "Newly diagnosed" | "New diagnosis")
fns.append("birads_4_5+pos.csv")

# Montage csv dump of 4s and 5s with KNOWN POSITIVE status
fns.append("birads_6+pos.csv")

# Output key file
key_fn = "mam_mr_key.csv"

# Local RedisCache project db
db_studies = 11
db_series  = 10

# proxy service
proxy_svc = "deathstar"
remote_aet = "gepacs"

# Sections to run
INIT_CACHE           = False
LOOKUP_UIDS          = False
CREATE_ANON_IDS      = False
COPY_FROM_PACS       = False
CREATE_KEY_CSV       = False


# ---------------------------------
# SCRIPT
# ---------------------------------

logging.basicConfig(level=logging.DEBUG)

with open("secrets.yml", 'r') as f:
    secrets = yaml.load(f)

R = RedisCache(db=db_studies, clear=INIT_CACHE)
Q = RedisCache(db=db_series, clear=INIT_CACHE)

proxy = None

if INIT_CACHE:
    # Merge positive and negative spreadsheets and indicate status
    for fn in fns:
        fp = os.path.join(data_root, fn)
        # set cancer_status from fn
        if fn.find("+pos") >= 0:
            cancer_status = "Postive"
        elif fn.find("+neg") >= 0:
            cancer_status = "Negative"
        else:
            cancer_status = "Unknown"
        # logging.debug("radcat: {}".format(radcat))
        M = CSVCache(fp, key_field="Accession Number")
        # Should study level dixels bc we are going to derive another cache
        # of series level elements next
        for k, v in M.cache.iteritems():
            v['CancerStatus'] = cancer_status
            d = Dixel(key=k, data=v, cache=R, remap_fn=Dixel.remap_montage_keys, dlvl=DLVL.STUDIES)


if LOOKUP_UIDS:
    proxy = Orthanc(**secrets['services'][proxy_svc])

    children_qs = [
        {'SeriesDescription': '*STIR*'},
        {'SeriesDescription': '1*MIN*SUB*'},
        {'SeriesDescription': '2*MIN*SUB*'},
        {'SeriesDescription': '6*MIN*SUB*'},
    ]

    lookup_child_uids(R, Q, children_qs, proxy, remote_aet, lazy=True)


if CREATE_ANON_IDS:
    set_anon_ids(Q, lazy=True)

# Where exactly are we putting this?  On disk for ML? on Hounsfield for review?  both?
if COPY_FROM_PACS:
    pass


if CREATE_KEY_CSV:
    create_key_csv(R, os.path.join(data_root, key_fn))
