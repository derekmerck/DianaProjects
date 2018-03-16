"""
U/S Thyroid Biopsy Cohort
Merck, Winter 2018

Uses DianaFuture

421 subjects with u/s biopsy and confirmed cancer status, 2016-2018

- Load Montage and Pathology spreadsheets and correlate subjects that
  exist in both datasets, i.e., patients with biopsy confirmed cancers.
- Lookup STUIDS and other patient data from PACS
- Assign anonymized id, name, dob
- Retrieve, anonymize, download, save
- Build out final metadata
"""
from DianaFuture.dcache import CSVCache, RedisCache
from DianaFuture.dixel import Dixel, DLVL
from DianaFuture.dapi import Orthanc
from GUIDMint import PseudoMint
import logging
import glob
import pytz
import os
import yaml
import hashlib
from pprint import pformat

# Script variables and section activation
data_root = "/Users/derek/Projects/Ultrasound/Thyroid Biopsy/data"
save_dir = "/Volumes/3dlab/thyroid_biopsy/anon"
remote_aet = "gepacs"

INIT_REDIS_CACHE     = False
LOOKUP_STUIDS        = False
CREATE_ANON_IDS      = False
COPY_FROM_PACS       = True
CLEAN_PROXY          = True
BUILD_CSV_FROM_REDIS = False

logging.basicConfig(level=logging.DEBUG)
with open("secrets.yml", 'r') as f:
    secrets = yaml.load(f)

R = RedisCache(db=15, clear=INIT_REDIS_CACHE)
O = Orthanc(**secrets['services']['deathstar'])

if INIT_REDIS_CACHE:

    # All Montage input
    fp = os.path.join(data_root, "montage_thyroid.csv")
    M = CSVCache(fp, key_field="AccessionNumber", remap_fn=Dixel.remap_montage_keys, autosave=False)

    def compare(M, P, R, status):

        for item in P.cache.itervalues():
            item['CancerStatus'] = status
        keys = ['PatientFirstName', 'PatientLastName']
        for m in M.cache.itervalues():
            mm = dict((k, m[k]) for k in keys if k in m)
            for p in P.cache.itervalues():
                pp = dict((k, p[k]) for k in keys if k in p)

                tm = m['StudyDate'].replace(tzinfo=pytz.UTC)
                tp = p['PathologyDate'].replace(tzinfo=pytz.UTC)

                # logging.debug("Checking for match: {} - {}".format(mm, pp))
                if (mm == pp) and abs((tp - tm).days) < 7:
                    # logging.debug("Found a match: {} - {}".format(mm, pp))

                    # logging.debug(tp - tm)

                    m.update(p)
                    # logging.debug(m)
                    R.put(m['AccessionNumber'], m)
                    break

    # Need a set of P's
    path_results = glob.glob(os.path.join(data_root, "path*.csv"))
    logging.debug(path_results)

    total_path = 0
    for p in path_results:
        P = CSVCache(p, key_field="PathologyCase", remap_fn=Dixel.remap_copath_keys)
        total_path = total_path + len(P.cache)
        status = "Positive" if "pos" in p else "Negative"
        compare(M, P, R, status)

    # Montage
    logging.debug("Montage total: {}".format(len(M.cache)))

    # Pathology
    logging.debug("Path total:    {}".format(total_path))

    # Intersection
    pos = [k for k,v in R.cache.iteritems() if v['CancerStatus']=="Positive"]
    logging.debug("Intsec total:  {}".format( len(R) ) )
    logging.debug("Intsec pos:    {}".format( len(pos) ) )
    logging.debug("Intsec %pos:   {}".format( ( float(len(pos))/float(len(R)) ) * 100 ) )


# LOOKUP SERUIDS
if LOOKUP_STUIDS:

    for key in R.keys():
        d = Dixel(key=key, cache=R)
        ret = O.find(d, remote_aet)
        if ret:
            # Take the first entry in ret and update the STUID/SERUID/INSTUID so we can retrieve
            d.data['StudyInstanceUID'] = ret[0].get("StudyInstanceUID")
            d.data['PatientName'] = ret[0].get("PatientName")
            d.data['PatientBirthDate'] = ret[0].get("PatientBirthDate")
            d.data['PatientSex'] = ret[0].get("PatientSex")
            if d.dlvl == DLVL.SERIES or d.dlvl == DLVL.INSTANCES:
                d.data['SeriesInstanceUID'] = ret[0].get("SeriesInstanceUID")
            if d.dlvl == DLVL.INSTANCES:
                d.data['SOPInstanceUID'] = ret[0].get("SOPInstanceUID")

            d.persist()


# CREATE ANON_IDS
if CREATE_ANON_IDS:

    mint = PseudoMint()
    for key in R.keys():
        d = Dixel(key=key, cache=R)

        if not d.data.get('PatientName'):
            logging.warn("Problem with MRN {}".format(d.data['PatientID']))
            continue

        name = d.data['PatientName']
        gender = d.data['PatientSex']
        dob = "-".join([d.data['PatientBirthDate'][0:4], d.data['PatientBirthDate'][4:6], d.data['PatientBirthDate'][6:8]])

        new_id = mint.pseudo_identity(name=name,
                                      gender=gender,
                                      dob=dob)

        d.data['AnonID']   = new_id[0]
        d.data['AnonName'] = new_id[1]
        d.data['AnonDoB']  = new_id[2]

        d.persist()


# COPY FROM PACS TO DISK
if COPY_FROM_PACS:

    def anon_fn(d):
        return {
            'Replace': {
                'PatientName': d.data['AnonName'],
                'PatientID': d.data['AnonID'],
                'PatientBirthDate': d.data['AnonDoB'].replace('-', ''),
                'AccessionNumber': hashlib.md5(d.data['AccessionNumber']).hexdigest(),
            },
            'Keep': ['PatientSex', 'StudyDescription', 'SeriesDescription'],
            'Force': True
        }

    for key in R.keys():
        d = Dixel(key=key, cache=R)

        if not d.data.get('AnonID'):
            logging.warn("No anon ID for MRN {}".format(d.data['PatientID']))
            continue

        fp = os.path.join(save_dir, d.data['AnonID'] + '.zip')
        if os.path.exists(fp):
            logging.debug('{} already exists -- skipping'.format(d.data['AnonID'] + '.zip'))
            continue

        # TODO: Check if file already exists for lazy!

        O.find(d, remote_aet, retrieve=True)

        # Check if it's there
        if not d in O:
            logging.warn("{} was not retrieved successfully!".format(d.data["AccessionNumber"]))

        r = O.anonymize(d, anon_fn(d))

        logging.debug(r)

        d.data['AnonOID'] = r['ID']
        d.persist()

        # Need an oid and a pname to save...
        e = Dixel(key=d.data['AnonOID'], data={'OID': d.data['AnonOID'], 'PatientID': d.data['AnonID']}, dlvl=DLVL.STUDIES)
        file_data = O.get(e, get_type='file')
        e.write_file(file_data, save_dir=save_dir)

        if CLEAN_PROXY:
            O.remove(d)
            O.remove(e)


# BUILD REPORT CORPUS

if BUILD_CSV_FROM_REDIS:
    N = CSVCache(os.path.join(data_root, "output.csv"),
                 key_field="AccessionNumber",
                 autosave=False, clear=True)

    for k in R.keys():
        d = Dixel(key=k, cache=R)
        # logging.debug(d.data)
        N.put(k, d.data)

    N.save_fn()
    logging.debug("Saved {} entries to {}".format(len(N), "output.csv"))
