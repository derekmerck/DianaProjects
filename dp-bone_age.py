"""
Bone Age Data Pull
Merck, Summer 2018

First project using diana_plus
Done 6/6/18 - 1370 of 1370

"""

# ---------------------------
# Keep this before any diana imports
from config import services
# ---------------------------

import os, logging, hashlib, datetime
from pprint import pprint
from diana.apis import MetaCache, Orthanc, DicomFile, BoneAgeReport
from diana.daemon import Porter

logging.basicConfig(level=logging.DEBUG)

# --------------
# SCRIPT CONFIG
# --------------

data_dir = "/Users/derek/Projects/Body/Bone Age/data"
input_fn = "dedicated_ba.csv"
key_fn   = "dedicated_ba.key.csv"
save_dir = "/Volumes/3dlab/bone_age_ai/ba_anon"

proxy_service = "proxy2"
proxy_domain = "gepacs"

INIT_CACHE = False
PULL_FROM_PACS = False

# Setup services
dixels = MetaCache()
proxy  = Orthanc(**services[proxy_service])
dicom_files  = DicomFile(location=save_dir)

# Load Montage format spreadsheet, find UIDs, set sham id
if INIT_CACHE:
    fp = os.path.join(data_dir, input_fn)
    dixels.load(fp, keymap=MetaCache.montage_keymap)

    for d in dixels:
        # Investigate to get UIDs
        proxy.find_item(d, proxy_domain)
        # set shams after investigation so you have complete dicom-format patient name
        d.set_shams()

    # Everything we need to create a key file
    fp = os.path.join(data_dir, key_fn)
    dixels.dump(fp)


fp = os.path.join(data_dir, key_fn)
dixels.load(fp)
for d in dixels:
    d.meta["chron_age"] = BoneAgeReport.chronological_age(d.report)
    d.meta["bone_age"]  = BoneAgeReport.skeletal_age(d.report)

    logging.debug(d.meta['chron_age'])
    logging.debug(d.meta['bone_age'])

fp = os.path.join(data_dir, "test.csv")
dixels.dump(fp)


# Exfiltrate, anonymize, stash to disk
if PULL_FROM_PACS:
    # Start from the key file or other cache
    fp = os.path.join(data_dir, key_fn)
    dixels.load(fp)

    P = Porter(source=proxy, dest=dicom_files, proxy_domain=proxy_domain)
    P.run(dixels)
