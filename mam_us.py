"""
Mam US Biopsy-Proven Cancer Status Cohort
Merck, Winter 2018

- Load Penrad spreadsheets
- Remove non 2012-2014 patients
- Load Path spreadsheets
- Find us patients by name and dob in path
- If there, status = pos, else status = neg
- Find accession num by MRN, date, modality in PACS
- Lookup UIDS and other patient data from PACS
- Assign anonymized id, name, dob
- Retrieve, anonymize, download, save
- Build out final metadata

Uses DianaFuture
"""

import logging, os, re, yaml
from dateutil import parser as dateparser
from datetime import timedelta, datetime
from pprint import pformat
from DianaFuture import CSVCache, RedisCache, Dixel, DLVL, Orthanc, \
    lookup_uids, lookup_child_uids, set_anon_ids, copy_from_pacs, create_key_csv

# ---------------------------------
# CONFIG
# ---------------------------------

data_root = "/Users/derek/Projects/Mammography/MR Breast ML/data/all_usbx"
dest_svc = "Hounsfield+MRBreastML"
save_dir = "/Volumes/3dlab/MRBreast/us/anon"

# All Penrad input
penrad_fn = "all_usbx_2012-2017.csv"

# All Path input
path_fn = "all_pos_2012-2014.csv"

# Output key file
key_fn = "mam_us_key.csv"

# Local RedisCache project db
db_studies = 11
db_series  = 10

# proxy service
proxy_svc = "deathstar"
remote_aet = "gepacs"

# Sections to run
INIT_CACHE            = False
LOOKUP_ACCESSION_NUMS = False
LOOKUP_CHILD_UIDS     = False
CREATE_ANON_IDS       = False
COPY_FROM_PACS        = False
CREATE_KEY_CSV        = False


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

    fp = os.path.join(data_root, penrad_fn)
    M = CSVCache(fp, key_field="PatientID")
    fp = os.path.join(data_root, path_fn)
    N = CSVCache(fp, key_field="CASE")

    def find_patient(penrad_v, path_cache):

        def like(penrad_v, path_v):
            # Adkins, Carolann M != BOOKERJORGE | LORENE | A
            penrad_name = penrad_v['PatientName'].replace(',','').split()
            penrad_lname = penrad_name[0].lower()
            penrad_fname = penrad_name[1].lower()
            penrad_study_date = dateparser.parse(penrad_v['ProcedureDate'])

            path_fname = path_v['PT FIRST'].lower()
            path_lname = path_v['PT LAST'].lower()
            path_study_date = dateparser.parse(path_v['ACCESSION DATE'])

            if  penrad_fname == path_fname and \
                penrad_lname == path_lname and \
                path_study_date - penrad_study_date < timedelta(days=10):
                logging.debug("Found {}".format(penrad_lname))
                return path_v['CASE']
            # logging.debug("Couldn't find {}".format(penrad_lname))
            return False

        for k, path_v in path_cache.cache.iteritems():
            # logging.debug("PENRAD:\n" + pformat(penrad_v))
            # logging.debug("PATH:\n"   + pformat(path_v))
            if like(penrad_v, path_v):
                return k
        return None

    for k, v in M.cache.iteritems():
        penrad_study_date = dateparser.parse(v['ProcedureDate'])
        if penrad_study_date < datetime(2012, 1, 1) or \
           penrad_study_date > datetime(2014, 3, 1):
            continue
        data = {}
        data['PatientID'] = v['PatientID']
        data['StudyDate'] = penrad_study_date.strftime("%Y%m%d")
        data['Modality'] = "US"
        data['PathCase'] = find_patient(v, N)
        d = Dixel(key=k, data=data, cache=R, dlvl=DLVL.STUDIES)

    key_fp = os.path.join(data_root, key_fn)
    create_key_csv(R, key_fp, key_field="PatientID")


if LOOKUP_ACCESSION_NUMS:

    proxy = Orthanc(**secrets['lifespan'][proxy_svc])
    lookup_uids(R, proxy, remote_aet)

    key_fp = os.path.join(data_root, key_fn)
    create_key_csv(R, key_fp, key_field="PatientID")

if LOOKUP_CHILD_UIDS:
    #Q .clear()
    proxy = Orthanc(**secrets['services'][proxy_svc])
    child_qs = [
        {'SeriesDescription': '*STIR*'},
        {'SeriesDescription': '1*MIN*SUB*'},
        {'SeriesDescription': '2*MIN*SUB*'},
        {'SeriesDescription': '6*MIN*SUB*'},
    ]

    lookup_child_uids(R, Q, child_qs, proxy, remote_aet)


if CREATE_ANON_IDS:
    set_anon_ids(Q, lazy=True)

# Where exactly are we putting this?  On disk for ML? on Hounsfield for review?  both?
if COPY_FROM_PACS:
    copy_from_pacs(Q, save_dir)


if CREATE_KEY_CSV:
    create_key_csv(R, os.path.join(data_root, key_fn))
