
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
key_fn1   = "mam_mr_mrbx_seruid.key.csv"

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
LOOKUP_ACCESSION_NUMS = False
LOOKUP_CHILD_UIDS   = False
RELOAD_CACHE        = False
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


if LOOKUP_ACCESSION_NUMS:

    proxy = Orthanc(**secrets['lifespan'][proxy_svc])
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
    proxy = Orthanc(**secrets[svc_domain][proxy_svc])
    child_qs = [
        {'SeriesDescription': '*STIR*'},
        {'SeriesDescription': '1*MIN*SUB*'},
        {'SeriesDescription': '2*MIN*SUB*'},
        {'SeriesDescription': '6*MIN*SUB*'},
    ]
    # We also want "t1_fl3d_tra_interVIEWS" series
    # Isometric pixels, best chance of seeing small features and margin

    lookup_child_uids(R, Q, child_qs, proxy, remote_aet)

    fp1 = os.path.join(data_root, key_fn1)
    create_key_csv(Q, fp1, key_field="SeriesInstanceUID")

if RELOAD_CACHE:
    fp1 = os.path.join(data_root, key_fn1)
    M = CSVCache(fp1, key_field="SeriesInstanceUID")
    for k, v in M.cache.iteritems():
        d = Dixel(key=k, data=v, cache=Q)

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