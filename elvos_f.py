"""
LVO Study Cohort
Merck, Winter 2018

Status: In progress, pending image inventory update

600+ subjects with ELVO studies and known stroke status, 2015?-2017

Workflow to scrape positives, age match normals, collect, anonymize, and download

- Load Montage spreadsheets
- Lookup UIDS and other patient data from PACS
- Assign anonymized id, name, dob
- Build out final metadata
- Retrieve, anonymize, download, save

Uses DianaFuture
"""

import logging, os, re, yaml, glob
from DianaFuture import CSVCache, RedisCache, Dixel, DLVL, Orthanc, Montage, \
    lookup_accessions, lookup_uids, set_anon_ids, copy_from_pacs, create_key_csv

logging.basicConfig(level=logging.DEBUG)

# ---------------------------------
# CONFIG
# ---------------------------------

data_root = "/Users/derek/Projects/Neuroimaging/CT ELVO AI/data"
save_dir = "/Volumes/3dlab/elvos/anon"

# All Montage input
fns = sorted(glob.glob(os.path.join(data_root,"*")))

# Output key file
key_fn = "elvo-604.csv"
key_fields = [
                "AccessionNumber",
                "PatientID",
                "PatientName",
                "PatientBirthDate"
                "StudyInstanceUID",
                "StudyDescription",
                "SeriesInstanceUID",
                "SeriesDescription",
                "PatientBirthDate",
                "AnonAccessionNumber",
                "AnonID",
                "AnonName",
                "AnonDoB",
                "ELVO on CTA?",
                "Gender",
                "status",
                "zip_avail"
                ]

# Local RedisCache project db
db = 11

# proxy service
proxy_svc = "deathstar"
remote_aet = "gepacs"

# Sections to run
INIT_CACHE           = False
LOOKUP_ACCESSIONS    = True
LOOKUP_UIDS          = False
CREATE_ANON_IDS      = False
CREATE_KEY_CSV       = True
COPY_FROM_PACS       = False

DRY_RUN = False  # Execute dl, copy, clean...


# ---------------------------------
# SCRIPT
# ---------------------------------

with open("secrets.yml", 'r') as f:
    secrets = yaml.load(f)

R = RedisCache(db=db, clear=INIT_CACHE)

proxy = None

if INIT_CACHE:
    # This is very complex b/c it was done over many many
    # iterations and with additions and removals, so we
    # essentially rebuild the history of the worklist
    # from scratch and allow dixels to exist in many statuses

    # Merge and update multiple spreadsheets and tag files
    for fp in fns:
        fn = os.path.split(fp)[1]
        if fn == key_fn:
            continue
        logging.debug(fn)

        match = re.search(r"\d mark (?P<flag>\w*) ?(?:by)? ?(?P<field>\w*)?.(?P<ext>\w*)", fn)

        flag = match.group('flag')
        field = match.group('field')
        ext = match.group('ext')

        if ext == "csv":
            M = CSVCache(fp, key_field="AccessionNumber")
            for k, v in M.cache.iteritems():
                try:
                    # Updating existing entry
                    rv = R.get(k)
                    rv['status'] = flag
                    rv.update(v)
                    R.put(k, rv, force=True)
                except TypeError:
                    # Creating new entry
                    v['status'] = flag
                    R.put(k, v)
        elif ext == "txt":
            with open(fp, 'rU') as f:
                keys = f.readlines()
                for key in keys:
                    key = key.rstrip()
                    if field=="AccessionNumber":
                        try:
                            v = R.get(k)
                            v['status'] = flag
                            R.put(k, v, force=True)
                        except ValueError:
                            logging.error("No data for {}".format(key))
                    else:
                        for k, v in R.cache.iteritems():
                            # logging.debug(k)
                            if v.get(field) and v[field] == key:
                                v['status'] = flag
                                R.put(k, v, force=True)
                                continue

    # Check for archive availability
    for k, v in R.cache.iteritems():
        if v.get('AnonID'):
            fn = v['AnonID'] + '.zip'  # Archive format
            fp = os.path.join(save_dir, fn)
            if os.path.exists(fp):
                v['zip_avail'] = True
            else:
                v['zip_avail'] = False
        else:
            v['zip_avail'] = False
        R.put(k, v, force=True)

if LOOKUP_ACCESSIONS:
    report_db = Montage(**secrets['services']['montage'])
    lookup_accessions(R, report_db)


# Need to do this for remaining "readys" and "candidates" (why not)
# This is actually a much more complex problem than usual -- want to
# detect the axial series with the most slices
if LOOKUP_UIDS:
    proxy = Orthanc(**secrets['services'][proxy_svc])
    lookup_uids(R, proxy, remote_aet, lazy=True)

# Anonymize 51083473
# set_anon_ids(dixel=Dixel(key="51083473", cache=R))

if CREATE_ANON_IDS:
    set_anon_ids(R, lazy=True)

if CREATE_KEY_CSV:
    create_key_csv(R, os.path.join(data_root, key_fn), key_fields=key_fields)

if COPY_FROM_PACS:
    if not proxy:
        proxy = Orthanc(**secrets['services'][proxy_svc])
    copy_from_pacs(proxy, remote_aet, R, save_dir, dry_run=DRY_RUN)
