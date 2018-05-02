"""
Lung Screening Study Historical Dose Pull
Merck, Spring 2018

Status: In-progress (pulling)

~700 subjects with missing dose information, 2015-2017

- Load our lscr worksheet and identify AccessinNum and PatientID
- For all GE scanners, lookup Series 997 with that AN and PID
- Pull to Deathstar and push to CIRR

Uses DianaFuture
"""

import os, logging, yaml, re
from DianaFuture import Orthanc, Dixel, DLVL, RedisCache, CSVCache, lookup_child_uids, create_key_csv
from DixelKit import StructuredTags
from pprint import pformat

# ---------------------------------
# CONFIG
# ---------------------------------

data_root = "/Users/derek/Projects/Body/CT Lung Screening"
fn="lscr_all_data1.csv"
key_fn = "lscr_all_data+series.csv"
key_fn1 = "lscr_all_data+dose.csv"

svc_domain = "lifespan"
proxy_svc = "deathstar"
remote_aet = "gepacs"
db=7

INIT_CACHE = False
LOOKUP_SERUIDS = False
COPY_FROM_PACS = True
READ_OUT_DOSE = True

# ---------------------------------
# SCRIPT
# ---------------------------------

logging.basicConfig(level=logging.DEBUG)

with open("secrets.yml", 'r') as f:
    secrets = yaml.load(f)

proxy = Orthanc(**secrets[svc_domain][proxy_svc])

R = RedisCache(db=db, clear=INIT_CACHE)
Q = RedisCache(db=db-1, clear=INIT_CACHE)

worklist = set()


if INIT_CACHE:

    fp = os.path.join(data_root, fn)
    M = CSVCache(fp, key_field="AccessionNumber")

    for k, v in M.cache.iteritems():
        data = {}
        data['PatientID'] = v['PatientID']
        data['AccessionNumber'] = v['AccessionNumber']
        d = Dixel(key=k, data=data, cache=R, dlvl=DLVL.STUDIES)

if LOOKUP_SERUIDS:

    Q.clear()

    proxy = Orthanc(**secrets['lifespan'][proxy_svc])

    child_qs = [
        {'SeriesNumber': '999'}
    ]

    lookup_child_uids(R, Q, child_qs, proxy, remote_aet)

    key_fp = os.path.join(data_root, key_fn)
    create_key_csv(Q, key_fp, key_field="AccessionNumber")

if COPY_FROM_PACS:

    for k in Q.cache.keys():
        d = Dixel(key=k, cache=Q, dlvl=DLVL.SERIES)
        if not d.data.get('StudyInstanceUID'):
            continue
        if not d in proxy:
            proxy.find(d, remote_aet, retrieve=True)

if READ_OUT_DOSE:

    for oid in proxy.inventory(DLVL.INSTANCES):

        d = Dixel(key=oid, data={'OID': oid}, dlvl=DLVL.INSTANCES)
        tags = proxy.get(d, 'tags')
        # logging.debug(pformat(tags))
        #
        # tags = StructuredTags.simplify_tags(tags)

        logging.debug(pformat(tags))

        accession_number = tags['AccessionNumber']
        dose_note = tags['CommentsOnRadiationDose']
        dlp = re.match(r"TotalDLP=(?P<val>\d+\.?\d+).*", dose_note).group('val')

        if tags.get("ExposureDoseSequence"):
            ctdi = tags['ExposureDoseSequence'][-1]['CTDIvol']
        else:
            ctdi = "UNKNOWN"

        e = Dixel(accession_number, cache=R)
        e.data['dlp'] = dlp
        e.data['ctdi'] = ctdi
        e.persist()

    key_fp = os.path.join(data_root, key_fn1)
    create_key_csv(R, key_fp, key_field="AccessionNumber")
