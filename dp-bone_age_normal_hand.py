"""
Bone Age Normal Hand Data Pull
Merck, Summer 2018

First project using diana_plus

"""

# ---------------------------
# Keep this before any diana imports
from config import services
# ---------------------------

import os, logging, hashlib, datetime
from pprint import pprint
from diana.apis import MetaCache, Orthanc, DicomFile
from diana.daemon import Porter

logging.basicConfig(level=logging.DEBUG)

# --------------
# SCRIPT CONFIG
# --------------

data_dir = "/Users/derek/Projects/Body/Bone Age/data"
input_fn = "normal_hand.csv"
key_fn   = "normal_hand.key.csv"
save_dir = "/Volumes/3dlab/bone_age_ai/norm_hand_anon"

proxy_service = "proxy1"
proxy_domain = "gepacs"

INIT_CACHE = False
PULL_FROM_PACS = True

# -------------

def set_shams(item):
    item.meta['ShamAccession'] = hashlib.md5(item.meta['AccessionNumber'].encode("UTF8"))
    item.meta['ShamName']      = hashlib.md5(item.meta['PatientID'].encode("UTF8"))
    item.meta['ShamID']        = hashlib.md5(item.meta['PatientID'].encode("UTF8"))
    item.meta['ShamDoB']       = datetime.date(year=1900, month=1, day=1)

# ------------


# Setup services
dixels = MetaCache()
proxy  = Orthanc(**services[proxy_service])
files  = DicomFile(location=save_dir)

# Load Montage format spreadsheet, find UIDs, set sham id
if INIT_CACHE:
    fp = os.path.join(data_dir, input_fn)
    dixels.load(fp, keymap=MetaCache.montage_keymap)

    for d in dixels:
        # Investigate to get UIDs
        proxy.find_item(d, proxy_domain)
        # set shams after investigation so you have complete dicom-format patient name
        set_shams(d)

    # Everything we need to create a key file
    fp = os.path.join(data_dir, key_fn)
    dixels.dump(fp)


# Exfiltrate, anonymize, stash to disk
if PULL_FROM_PACS:
    # Start from the key file or other cache
    fp = os.path.join(data_dir, key_fn)
    dixels.load(fp)

    P = Porter(source=proxy, dest=files, proxy_domain=proxy_domain, explode=(1,1))
    P.run(dixels)
