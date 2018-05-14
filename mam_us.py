"""
Mam US w Biopsy-Proven Cancer Status Cohort
Merck, Spring 2018

- Load Penrad spreadsheets of usbx studies
  - MISSING accession num, report, HAS mrn, data, pfirst+last, date of service
  - Added 'penrad id' as key b/c no a/n until later, using MRN misses 500+ repeat studies
  - Remove before 2012 and after 2017 patients

- Load Path spreadsheets
  - MISSING mrn, HAS cancer status, pfirst+last, date or year of service

- Match us studies by name and date or year of service in path
  - If present, status = pos, else status = neg
  - Assign anonymized study id hash

- Find accession num, UUID by MRN, date, modality in PACS

- Retrieve from PACS, download, crop PHI and save as PNG

Uses DianaFuture
"""

import logging, os, re, yaml, hashlib
from dateutil import parser as dateparser
from datetime import timedelta, datetime
from pprint import pformat
from requests import ConnectionError
from DianaFuture import CSVCache, RedisCache, Dixel, DLVL, Orthanc, \
    lookup_uids, create_key_csv

# ---------------------------------
# CONFIG
# ---------------------------------

data_root = "/Users/derek/Projects/Mammography/MR Breast ML/data/all_usbx"
save_root = "/Users/derek/Dropbox (Brown)/USbx_anon"
err_logfile = "usbx_failure_log.txt"

# All Penrad input
penrad_fn = "all_usbx_2012-2017.csv"

# All Path input
path_fns = ["all_pos_2012-2014.csv", "all_pos_2014-2017.csv"]

# Output key file
key_fn = "mam_usbx.key.csv"

# Local RedisCache project db
db_studies = 11
db_series  = 10

# proxy service
proxy_svc = "deathstar"
remote_aet = "gepacs"

# Sections to run
INIT_CACHE            = False
RELOAD_CACHE          = False
LOOKUP_ACCESSION_NUMS = False
COPY_FROM_PACS        = True


# ---------------------------------
# SCRIPT
# ---------------------------------

logging.basicConfig(level=logging.DEBUG)

with open("secrets.yml", 'r') as f:
    secrets = yaml.load(f)

R = RedisCache(db=db_studies, clear=( INIT_CACHE or RELOAD_CACHE ) )
Q = RedisCache(db=db_series, clear=( INIT_CACHE or RELOAD_CACHE ))

proxy = None

if INIT_CACHE:

    fp = os.path.join(data_root, penrad_fn)
    M = CSVCache(fp, key_field="PenradID")

    for path_fn in path_fns:
        fp = os.path.join(data_root, path_fn)
        N = CSVCache(fp, key_field="CASE")
        for k,v in N.cache.iteritems():
            Q.put(k, v)
            logging.debug('{}: {}'.format(path_fn, k))

    def find_patient(penrad_v, path_cache):

        def like(penrad_v, path_v):
            penrad_name = penrad_v['PatientName'].replace(',','').split()
            penrad_lname = penrad_name[0].lower()
            penrad_fname = penrad_name[1].lower()
            penrad_study_date = dateparser.parse(penrad_v['ProcedureDate'])

            path_fname = path_v['PT FIRST'].lower()
            path_lname = path_v['PT LAST'].lower()
            path_case = path_v.get('CASE')
            if path_v.get('ACCESSION DATE'):
                path_study_date = dateparser.parse(path_v['ACCESSION DATE'])
                date_match = "exact dates"
            else:
                m = re.match(r"RS(\d{2})-\d{2,}", path_case)

                try:
                    path_year = int(m.group(1))
                except AttributeError:
                    logging.error(path_case)
                    exit()

                path_study_date = datetime(year=2000+path_year, month=1, day=1)
                penrad_study_date = datetime(year=penrad_study_date.year, month=1, day=1)
                date_match = "same years"

                # logging.debug(path_study_date)
                # logging.debug(penrad_study_date)

            if  penrad_fname == path_fname and \
                penrad_lname == path_lname and \
                path_study_date - penrad_study_date < timedelta(days=10):
                logging.debug("Found {} in POS by {}".format(penrad_lname.capitalize(), date_match))
                return path_case
            # logging.debug("Couldn't find {}".format(penrad_lname))
            return False

        for k, path_v in path_cache.cache.iteritems():
            # logging.debug("PENRAD:\n" + pformat(penrad_v))
            # logging.debug("PATH:\n"   + pformat(path_v))
            if like(penrad_v, path_v):
                return k

        penrad_name = penrad_v['PatientName'].replace(',','').split()
        penrad_lname = penrad_name[0].capitalize()
        logging.debug("Could not find {}, so NEG".format(penrad_lname))
        return None

    for k, v in M.cache.iteritems():
        penrad_study_date = dateparser.parse(v['ProcedureDate'])
        if penrad_study_date < datetime(2012, 1, 1) or \
           penrad_study_date > datetime(2017, 12, 31):
            continue
        data = {}
        data['PatientID'] = v['PatientID']
        data['StudyDate'] = penrad_study_date.strftime("%Y%m%d")
        data['Modality'] = "US"
        data['PathCase'] = find_patient(v, Q)
        data['AnonID'] = hashlib.md5(v['PatientID']).hexdigest()[:16]

        d = Dixel(key=k, data=data, cache=R, dlvl=DLVL.STUDIES)

    key_fp = os.path.join(data_root, key_fn)
    create_key_csv(R, key_fp, key_field="PenradID")

if LOOKUP_ACCESSION_NUMS:

    proxy = Orthanc(**secrets['lifespan'][proxy_svc])
    # Get accession num, study UUID
    lookup_uids(R, proxy, remote_aet)

    key_fp = os.path.join(data_root, key_fn)
    create_key_csv(R, key_fp, key_field="PenradID")

if RELOAD_CACHE:
    # If Redis is overwritten or cleared, but key exists

    fp = os.path.join(data_root, key_fn)
    M = CSVCache(fp, key_field="PenradID")
    for k,v in M.cache.iteritems():
        Dixel(key=k, data=v, cache=R, dlvl=DLVL.STUDIES)

if COPY_FROM_PACS:
    # Deep copy -- retrieve study, move data by instance b/c we need to process each one

    proxy = Orthanc(**secrets['lifespan'][proxy_svc])

    for k,v in R.cache.iteritems():
        d = Dixel(k, cache=R)

        if d.data.get('complete'):
            continue

        if d.data['PathCase'] == "None":
            status = "neg"
        else:
            status = "pos"

        try:
            proxy.find(d, 'gepacs', retrieve=True)
            ser_oids = proxy.get(d, get_type='info')['Series']
        except (ConnectionError, KeyError), e:
            prid = k
            an = d.data.get('AccessionNumber', 'UNK ACCESSION')
            mrn = d.data.get('PatientID', 'UNK MRN')
            info = ":".join([prid, mrn, an])
            logging.error("Failed to process {}".format(info))
            with open("usbx_failures_log.txt", "a") as f:
                f.write(info + '\n' )
                continue

        for ser_oid in ser_oids:
            e = Dixel(key=ser_oid, data={'OID': ser_oid}, dlvl=DLVL.SERIES)
            inst_oids = proxy.get(e, get_type="info")['Instances']
            for inst_oid in inst_oids:
                f = Dixel(key=inst_oid, data={'OID': inst_oid}, dlvl=DLVL.INSTANCES)
                save_dir = os.path.join(save_root, status, d.data['AnonID'])

                # check if file exists before we grab it
                fp = os.path.join(save_dir, '{}.png'.format(d.oid()))
                if os.path.isfile(fp):
                    logging.warn("{} already exists, skipping".format(d.oid()))
                    continue

                file_data = proxy.get(f, get_type='file')
                f.write_image(file_data, save_dir=save_dir)

        # All done with this one, don't bother grabbing it again
        d.data['complete'] = True
        d.persist()