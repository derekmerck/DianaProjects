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

import logging, yaml
from DianaFuture import Orthanc, Dixel, DLVL, RedisCache, CSVCache
from DixelKit import StructuredTags
from pprint import pformat

# ---------------------------------
# CONFIG
# ---------------------------------

fp="/Users/derek/Desktop/lscr_all_data1.csv"

svc_domain = "lifespan"
proxy_svc = "deathstar"
dest_svc = "cirr1"
db=7

# ---------------------------------
# SCRIPT
# ---------------------------------

logging.basicConfig(level=logging.DEBUG)

with open("secrets.yml", 'r') as f:
    secrets = yaml.load(f)

proxy = Orthanc(**secrets[svc_domain][proxy_svc])
dest  = Orthanc(**secrets[svc_domain][dest_svc])

R = RedisCache(db=db)
Q = RedisCache(db=db-1)

worklist = set()

for k in R.cache.keys():
    v = R.cache.get(k)

    if not v.get("StudyInstanceUID"):
        continue

    d = Dixel(k, data=v)

    info = proxy.get(d, 'info')
    instances = info['Instances']

    if len(instances) == 1:
        inst_oid = instances[0]
        e = Dixel(inst_oid, data=d.data, cache=Q, dlvl=DLVL.INSTANCES)
        e.data['OID'] = inst_oid
        tags = proxy.get(e)

        st = StructuredTags.simplify_tags(tags)

        logging.debug(pformat(st))

exit()


# M = CSVCache(fp, key_field="AccessionNumber")
#
# for k, v in M.cache.iteritems():
#     if v["StationManufacturer"].startswith("General"):
#         v['SeriesNumber'] = "997"
#         d = Dixel(key=k, data=v, cache=R, dlvl=DLVL.SERIES)
#         d.persist()
#         worklist.add(d)

# data = {"AccessionNumber": "50072977",
#         "PatientID": "10006579999",
#         "SeriesNumber": "997"}

# d = Dixel("50072977", data=data, dlvl=DLVL.SERIES, cache=R)
# d.persist()

for d in worklist:
    ret = proxy.find(d, "gepacs", retrieve=True)
    if ret:
        # Take the first entry in ret and update the STUID/SERUID/INSTUID so we can retrieve
        d.data['StudyInstanceUID'] = ret[0].get("StudyInstanceUID")
        d.data['PatientName'] = ret[0].get("PatientName")
        d.data['PatientBirthDate'] = ret[0].get("PatientBirthDate")
        d.data['PatientSex'] = ret[0].get("PatientSex")
        if d.dlvl == DLVL.SERIES or d.dlvl == DLVL.INSTANCES:
            d.data['SeriesInstanceUID'] = ret[0].get("SeriesInstanceUID")
        if d.dlvl == DLVL.SERIES:
            d.data['SeriesDescription'] = ret[0].get("SeriesDescription")
            d.data['SeriesNumber'] = ret[0].get("SeriesNumber")
            d.data['SeriesNumInstances'] = ret[0].get('NumberOfSeriesRelatedInstances')
        if d.dlvl == DLVL.INSTANCES:
            d.data['SOPInstanceUID'] = ret[0].get("SOPInstanceUID")

        d.persist()

        proxy.copy(d, dest )
        d.data['DONE'] = True
        d.persist()
