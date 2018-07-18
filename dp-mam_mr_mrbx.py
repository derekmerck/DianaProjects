


# ---------------------------
# Keep this before any diana imports
from config import services
# ---------------------------

import os, logging
from pprint import pprint
import attr
from pprint import pformat
from diana.apis import MetaCache, MammographyReport, Dixel, Orthanc
from diana.utils import DicomLevel, dicom_strfdate, dicom_strfname
from diana.daemon.porter import Porter, ProxyGetMixin, PeerSendMixin
from guidmint import PseudoMint
import hashlib
import datetime

logging.basicConfig(level=logging.DEBUG)

# --------------
# SCRIPT CONFIG
# --------------

data_dir0 = "/Users/derek/Projects/Mammography/MR Breast ML/data/mr_prior_mrbx"
input_fn0   = "mam_mr_mrbx_seruid.key.csv"

data_dir1 = "/Users/derek/Projects/Mammography/MR Breast ML/data/mr_prior_bx"
input_fn1 = "all_candidates+seruids.csv"

key_fn    = "mam_mr_mrbx.key.csv"
peer_dest = "hounsfield-mam"

# save_dir = "/Volumes/3dlab/bone_age_ai/norm_hand_anon"

proxy_service = "proxy1"
proxy_domain = "gepacs"

INIT_CACHE = False
COPY_FROM_PACS = True

# Setup services
dixels0 = MetaCache()
dixels1 = MetaCache()
proxy   = Orthanc(**services[proxy_service])
dest    = Orthanc(**services[peer_dest])

mint = PseudoMint()

# Load Montage format spreadsheet, merge with birads 6 from MR_anybx
if INIT_CACHE:
    # MR plus MRbx
    fp = os.path.join(data_dir0, input_fn0)
    dixels0.load(fp, level=DicomLevel.SERIES)

    for d in dixels0:
        try:
            birads = MammographyReport(d.report).birads()
            logging.debug("birads {}".format(birads))
        except ValueError:
            birads = -1
            logging.warning("No birads indicated")
        d.meta['birads'] = birads
        d.meta['batch'] = 1

    # MR plus anybx -- need to collect only birads 6s
    fp = os.path.join(data_dir1, input_fn1)
    dixels1.load(fp, level=DicomLevel.STUDIES)

    # For each study in dixels1
    #   - check the birads
    #   - if it's a 6
    #     - split it up
    #     - add each series to dixels0

    birads6 = 0
    for d in dixels1:

        # logging.debug(pformat(d.meta))

        try:
            birads = MammographyReport(d.report).birads()
            logging.debug("birads {}".format(birads))
        except ValueError:
            logging.warning("No birads indicated")

        d.meta['birads'] = birads

        if birads == "6":
            birads6 += 1
            # Create 4 new SERIES level dixels
            for postfix in ["+STIR", "+1MS", "+2MS", "6MS"]:
                meta = {
                    "AccessionNumber": d.meta["AccessionNumber"],
                    "OrderCode": d.meta["Exam Code"],
                    "Organization": "",
                    "PatientAge": d.meta["Patient Age"],
                    "PatientBirthDate": None,
                    "PatientID": d.meta["PatientID"+postfix],
                    "PatientName": None,
                    "PatientSex": None,
                    "SeriesDescription": d.meta["SeriesDescription"+postfix],
                    "SeriesInstanceUID": d.meta["SeriesInstanceUID"+postfix],
                    "SeriesNumber": d.meta["SeriesNumber"+postfix],
                    "StudyDate": d.meta["StudyDate"+postfix],
                    "StudyDescription": None,
                    "StudyInstanceUID": d.meta["StudyInstanceUID"+postfix],
                    "birads": birads,
                    'batch': 2
                }

                did = (meta["AccessionNumber"], meta["SeriesDescription"])

                e = Dixel(uid=did, meta=meta, level=DicomLevel.SERIES, report=d.report)
                dixels0.put(e)

    print("BIRADS 6s: {}".format(birads6))

    for d in dixels0:
        # Investigate to get UIDs
        d = proxy.find_item(d, proxy_domain)
        # set shams _after_ investigation b/c need the complete dicom-format patient name
        if d:
            d.set_shams()
        # pprint(d.meta)

    # Have everything we need to create a key file
    fp = os.path.join(data_dir0, key_fn)
    dixels0.dump(fp)


if COPY_FROM_PACS:

    fp = os.path.join(data_dir0, key_fn)
    dixels0.load(fp, level=DicomLevel.SERIES)

    @attr.s
    class MyPorter(ProxyGetMixin, PeerSendMixin, Porter): pass

    p = MyPorter(source=proxy, proxy_domain=proxy_domain, dest=dest, peer_dest=peer_dest)
    p.run2(dixels0)


exit()






import logging, os, re, yaml
from DianaFuture import CSVCache, RedisCache, Dixel, DLVL, Orthanc, \
    lookup_uids, lookup_child_uids, set_anon_ids, copy_from_pacs, create_key_csv


# ---------------------------------
# CONFIG
# ---------------------------------

data_root   = "/Users/derek/Projects/Mammography/MR Breast ML/data/mr_prior_mrbx"
save_root   = "/Volumes/3dlab/mam_ai/MR_Mrbx_anon"
err_logfile = "mr_mrbx_failure_log.txt"

# Montage input
input_fn0 = "mr_prior_to_mrbx.csv"
input_fn1 = "mam_mr_mrbx_uuid.hand.csv"
input_fn2 = "mam_mr_mrbx_seruid.csv"

# Output key file
key_fn0    = "mam_mr_mrbx.complete.key.csv"
key_fn1   = "mam_mr_mrbx_seruid2.key.csv"

# Local RedisCache project db
db_studies = 13
db_series  = 12

# proxy service
svc_domain = "lifespan"
proxy_svc  = "deathstar"
remote_aet = "gepacs"
dest_svc = "hounsfield+mam"

# Sections to run
INIT_CACHE          = False
RELOAD_CACHE        = True
LOOKUP_ACCESSION_NUMS = True
LOOKUP_CHILD_UIDS   = True
COPY_FROM_PACS      = True


# ---------------------------------
# SCRIPT
# ---------------------------------

logging.basicConfig(level=logging.DEBUG)

with open("secrets.yml", 'r') as f:
    secrets = yaml.load(f)
    services = secrets['services'][svc_domain]

R = RedisCache(db=db_studies, clear=(INIT_CACHE or RELOAD_CACHE))
Q = RedisCache(db=db_series, clear=(INIT_CACHE or RELOAD_CACHE))

proxy = None

if INIT_CACHE:
    fp0 = os.path.join(data_root, input_fn0)
    M = CSVCache(fp0, key_field="Accession Number")
    for k, v in M.cache.iteritems():
        if v["Primary Match"] == "TRUE":

            # For old studies, PatientID and AccessionNumber are likely bogus
            if v["Exam Code"] == "RAD80221591" and v["Accession Number"][0] != "R":
                accession_num = "R" + v["Accession Number"]
                v["Accession Number"] = accession_num
                v["Patient MRN"] = ''
                logging.debug("NEW AN: {}".format(accession_num))
                k = accession_num

            d = Dixel(key=k, data=v, cache=R, remap_fn=Dixel.remap_montage_keys, dlvl=DLVL.STUDIES)
    # Down to 234


if RELOAD_CACHE:
    fp1 = os.path.join(data_root, key_fn1)
    M = CSVCache(fp1, key_field="AccessionNumber")
    for k, v in M.cache.iteritems():
        d = Dixel(key=k, data=v, cache=R)

if LOOKUP_ACCESSION_NUMS:

    proxy = Orthanc(**services[proxy_svc])
    # Get accession num, study UUID
    lookup_uids(R, proxy, remote_aet)

    fp0 = os.path.join(data_root, key_fn0)
    create_key_csv(R, fp0, key_field="AccessionNumber")

    # Down to 176
    # Down to 89

    # Resolved:
    # - Add an R to old RIH a/ns, add an N to old NPH a/ns
    # - Remove PatientID - often old ones are incorrect
    # - For RMR6398, the a/n with images is always 1 less (that's the cad)



if LOOKUP_CHILD_UIDS:
    proxy = Orthanc(**services[proxy_svc])
    child_qs = [
        {'SeriesDescription': '*STIR*'},
        {'SeriesDescription': '[1I]*MIN*SUB*'},
        {'SeriesDescription': '2*MIN*SUB*'},
        {'SeriesDescription': '6*MIN*SUB*'},
    ]
    # We also want "t1_fl3d_tra_interVIEWS" series
    # Isometric pixels, best chance of seeing small features and margin

    lookup_child_uids(R, Q, child_qs, proxy, remote_aet)

    fp1 = os.path.join(data_root, key_fn1)
    create_key_csv(Q, fp1, key_field="SeriesInstanceUID")

if COPY_FROM_PACS:
    # Deep copy -- retrieve study, move data by instance b/c we need to process each one

    proxy = Orthanc(**services[proxy_svc])
    dest =  Orthanc(**services[dest_svc])

    for k,v in Q.cache.iteritems():

        # Missing data during series lookup
        if not v.get("SeriesInstanceUID"):
            d.data['complete'] = 'Incomplete UID'
            continue

        d = Dixel(k, cache=Q)

        # # Reset all
        # d.data['complete'] = ''
        # d.persist()
        # continue

        if d in dest:
            logging.warning("{} already in dest".format(d.data.get("AccessionNumber")))
            d.data['complete'] = "complete"
            d.persist()
            continue

        # Skip if there is any entry in "complete"
        # if d.data.get('complete') and d.data.get('complete') != "requested":
        #     logging.warning("{} previously noted as {}".format(d.data.get("AccessionNumber"), d.data.get('complete')))
        #     continue

        try:
            if d not in proxy:
                proxy.find(d, 'gepacs', retrieve=True)
        except:
            if d.data['complete'] == "requested":
                logging.error("Failed to find {}".format(d.data.get("AccessionNumber")))
                d.data['complete'] = "unretrievable"
            else:
                logging.error("Waiting on return".format(d.data.get("AccessionNumber")))
                d.data['complete'] = "requested"
            d.persist()
            continue

        try:
            proxy.copy(d, dest)
        except:
            logging.error("Failed to copy {}".format(d.data.get("AccessionNumber")))
            d.data['complete'] = "uncopiable"
            d.persist()
            continue

        # All done with this one, don't want to bother grabbing it again
        d.data['complete'] = "complete"
        d.persist()