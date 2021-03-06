"""
Chest AI Pathology Cohort
Merck, Summer 2018

Status: In-progress (pulling)

~17k subjects with RADCAT'd P/A chest CR, 2017-2018

- Load Montage spreadsheets and label by radcat
- Lookup UIDS and other patient data from PACS
- Assign anonymized id, name, dob
- Build out final metadata key
- Retrieve, anonymize, download, save

Revised to use diana_plus
"""

# $ docker run -d  -v /home/derek/config/cr_chest:/etc/orthanc -p 8103:8042 --rm --name chest_cr --env WVB_ENABLED=true osimis/orthanc

# ---------------------------
# Keep this before any diana imports
from tests.config import services
# ---------------------------

import os, logging
from diana.apis import MetaCache, Orthanc, DicomFile
from diana.daemon import Porter
from diana.daemon.porter import ProxyGetMixin, PeerSendMixin

logging.basicConfig(level=logging.DEBUG)

# --------------
# SCRIPT CONFIG
# --------------

data_dir = "/Users/derek/Projects/Body/Chest CR/data"

# Validation source data
# Images given radcat, no movson or agarwal
val_fns = ["chest_cr_rad1.csv", "chest_cr_rad4.csv"]
val_key_fn = "chest_cr_val.key.csv"

# Training data
# All images, fa17 -> sp18
key_fn   = "chest_cr_17k.key.csv"
save_dir = "/Volumes/3dlab/data/chest_cr_val_anon"

proxy_service  = "proxy1"
proxy_domain   = "gepacs"
review_service = "hounsfield-chcr"

SUBSELECT = False
PULL_AND_SAVE = False
PULL_AND_SEND = True

# Setup services
proxy  = Orthanc(**services[proxy_service])

# -------------

if SUBSELECT:
    n = 100

    val_key = MetaCache( location=os.path.join(data_dir, val_key_fn) )

    for fn in val_fns:
        dixels = MetaCache( location=os.path.join(data_dir, fn) )
        dixels.load( keymap=MetaCache.montage_keymap )

        for i in range(n):
            d = dixels.select_random()
            e = proxy.find_item(d, domain=proxy_domain)
            d.set_shams()
            d.meta['radcat'] = d.report.radcat()
            val_key.put(d)

    val_key.dump()



# Exfiltrate, anonymize, stash to disk
if PULL_AND_SAVE:

    dixels = MetaCache()
    files = DicomFile(location=save_dir)

    # Start from the key file or other cache
    fp = os.path.join(data_dir, val_key_fn)
    dixels.load(fp)

    P = Porter(source=proxy, dest=files, proxy_domain=proxy_domain, explode=(1,2))
    P.run(dixels)

if PULL_AND_SEND:

    dixels = MetaCache()
    files = DicomFile(location=save_dir)

    # Start from the key file or other cache
    fp = os.path.join(data_dir, val_key_fn)
    dixels.load(fp)

    class PeerPorter(Porter, PeerSendMixin, ProxyGetMixin):
        pass

    P = PeerPorter(source=proxy, proxy_domain=proxy_domain, peer_dest=review_service)
    logging.info(P)

    P.run2(dixels)


# # ---------------------------------
# # CONFIG
# # ---------------------------------
#
# data_root = "/Users/derek/Projects/Body/XR Chest AI/data/"
#
# # All Montage input
# fns = ['chestai-1s.csv', 'chestai-4s.csv']
#
# # Option to init by reading key?
# if INIT_CACHE:
#     # Merge positive and negative spreadsheets and indicate status
#     for fn in fns:
#         fp = os.path.join(data_root, fn)
#         radcat = re.findall("chestai-(\d)s\.csv", fn)[0]  # set radcat from fn
#         # logging.debug("radcat: {}".format(radcat))
#         M = CSVCache(fp, key_field="Accession Number")
#         # Should series level dixels bc we only want the AP/PAs, however
#         # they are named inconsistently, so have to pull the whole study
#         for k, v in M.cache.iteritems():
#             v['radcat'] = radcat
#             # v['SeriesDescription'] = "CHEST AP\PA"  # Query string for later
#             d = Dixel(key=k, data=v, cache=R, remap_fn=Dixel.remap_montage_keys, dlvl=DLVL.STUDIES)
#
#
# # This takes ~15 mins
# if LOOKUP_UIDS:
#     proxy = Orthanc(**secrets[service_domain][proxy_svc])
#     lookup_uids(R, proxy, remote_aet, lazy=True)
#
#
# if CREATE_ANON_IDS:
#     set_anon_ids(R, lazy=True)
#     for k, v in M.cache.iteritems():
#         v['AnonAccessionNum'] = hashlib.md5(v["AccessionNumber"]).hexdigest()
#         v['status'] = 'ready'
#         d = Dixel(key=k, data=v, cache=R)
#
