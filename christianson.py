
import logging, os, re, yaml
from DianaFuture import CSVCache, RedisCache, Dixel, DLVL, Orthanc, \
    lookup_uids, lookup_child_uids, set_anon_ids, copy_from_pacs, create_key_csv


# ---------------------------------
# CONFIG
# ---------------------------------

# Local RedisCache project db
db_studies = 15

# proxy service
svc_domain = "lifespan"
proxy_svc  = "deathstar"

dest_domain = "do"
dest_svc = "christianson"

# Sections to run
INIT_CACHE          = False
LOOKUP_ACCESSION_NUMS = True
COPY_FROM_PACS      = True


# ---------------------------------
# SCRIPT
# ---------------------------------

logging.basicConfig(level=logging.DEBUG)

with open("secrets.yml", 'r') as f:
    secrets = yaml.load(f)
    services = secrets['services'][svc_domain]

# mrn = "10018442582"

ans = ["R13189164", "R13391094", "51543370"]

oids = ["ba381466-f680593b-7972942a-dc0859a0-1d0669b2",
        "189da010-b440f7c9-fc72931e-5dc0f5dd-199dc622",
        "60236ab4-8e72e833-88f6e7f4-ae6d8a1d-28d28602"]

anon_oids = ["dc661764-0d23e8d6-32118810-174062f7-aa4c1a80",
             "33c45558-4bfb8e44-3994a51d-2b0e5bb2-d606f4e7",
             "5cf91699-60d6e823-1dbd74ff-2338a113-ac16cf4f"]

proxy = Orthanc(**services[proxy_svc])

# for k in ans:
#
#     d=Dixel(key=k, data={"AccessionNumber": k} )
#
#     ret = proxy.find(d, remote_aet, retrieve=True)
#     if ret:
#         # Take the first entry in ret and update the STUID/SERUID/INSTUID so we can retrieve
#         if not d.data.get("AccessionNumber"):
#             d.data['AccessionNumber'] = ret[0].get("AccessionNumber")
#         d.data['StudyInstanceUID'] = ret[0].get("StudyInstanceUID")
#         d.data['PatientID'] = ret[0].get("PatientID")
#         d.data['PatientName'] = ret[0].get("PatientName")
#         d.data['PatientBirthDate'] = ret[0].get("PatientBirthDate")
#         d.data['PatientSex'] = ret[0].get("PatientSex")
#         if d.dlvl == DLVL.SERIES or d.dlvl == DLVL.INSTANCES:
#             d.data['SeriesInstanceUID'] = ret[0].get("SeriesInstanceUID")
#         if d.dlvl == DLVL.SERIES:
#             d.data['SeriesDescription'] = ret[0].get("SeriesDescription")
#             d.data['SeriesNumber'] = ret[0].get("SeriesNumber")
#             d.data['SeriesNumInstances'] = ret[0].get('NumberOfSeriesRelatedInstances')
#         if d.dlvl == DLVL.INSTANCES:
#             d.data['SOPInstanceUID'] = ret[0].get("SOPInstanceUID")
#
#     with open("christianson.txt", "a") as f:
#         f.writelines( d.oid() )

# from GUIDMint import PseudoMint
# from hashlib import md5
#
# mint = PseudoMint()
# def get_anon_id(d):
#     # logging.debug(pformat(d.meta))
#
#     name = (d.data["PatientName"]).upper()
#     if d.data.get('PatientSex'):
#         gender = d.data['PatientSex']
#     elif d.data.get("Gender"):
#         gender = d.data['Gender'][0].upper()
#
#     # if d.data.get('PatientAge'):
#     #     age = int(d.data['PatientAge'][0:3])
#     #     new_id = mint.pseudo_identity(name=name,
#     #                                   gender=gender,
#     #                                   age=age)
#
#     if d.data.get('PatientBirthDate'):
#         dob = d.data.get('PatientBirthDate')
#         dob = dob[:4] + '-' + dob[4:6] + '-' + dob[6:]
#         new_id = mint.pseudo_identity(name=name,
#                                       gender=gender,
#                                       dob=dob)
#
#     else:
#         logging.warn("Not enough meta-data for {}, need to lookup".format(d))
#         return d
#
#
#     d.data['AnonID'] = new_id[0]
#     d.data['AnonName'] = new_id[1]
#     d.data['AnonDoB'] = new_id[2].replace('-', '')
#     d.data['AnonAccessionNumber'] = md5(d.data['AccessionNumber']).hexdigest()
#     return d


# for k in oids:
#
#     d=Dixel(key=k, data={"OID": k}, dlvl=DLVL.STUDIES)
#     ret=proxy.get(d)
#     d.data.update(ret)
#
#     d = get_anon_id(d)
#
#     ret = proxy.anonymize(d, Orthanc.simple_anon_map)
#
#     print(ret['ID'])
#
#     with open("christianson_anon.txt", "a") as f:
#         f.writelines( ret['ID'] )

for k in anon_oids:

    d=Dixel(key=k, data={"OID": k}, dlvl=DLVL.STUDIES)

    proxy.copy(d, "christianson")
