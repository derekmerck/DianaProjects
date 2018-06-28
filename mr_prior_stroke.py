"""
Incidental MR prior to Stroke Cohort
Merck, Spring 2018

Cutting

"""

import logging, yaml, os, hashlib
from DianaFuture import RedisCache, CSVCache, Orthanc, Dixel, DLVL, lookup_uids, create_key_csv, set_anon_ids, copy_from_pacs
from random import shuffle

# ---------------------------------
# CONFIG
# ---------------------------------

data_root   = "/Users/derek/Projects/Neuroimaging/MR prior stroke/data"
save_root   = "/Volumes/3dlab/stroke_ai/prior_mr_anon2"

# Montage input
input_fn      = "epic_cohort.csv"
candidates_fn = "control_candidates.csv"
controls_fn   = "controls.csv"

# Output key file
key_fn = "stroke_ai.key.csv"
control_key_fn = "stroke_ai.controls_key.csv"

# Local RedisCache project db
db_studies = 9

# proxy service config
svc_domain = "lifespan"
proxy_svc  = "deathstar1"
remote_aet = "gepacs"

# Sections to run
INIT_CACHE          = False
SELECT_CONTROLS     = False
ADD_POST_STUDIES    = False
LOOKUP_UIDS         = False
SET_ANON_IDS        = False
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

proxy = None

if INIT_CACHE:

    fp = os.path.join(data_root, input_fn)
    M = CSVCache(fp, key_field="AccessionNumber")
    for k, v in M.cache.iteritems():
        d = Dixel(key=k, data=v, cache=R, dlvl=DLVL.STUDIES)

    fp = os.path.join(data_root, key_fn)
    create_key_csv(R, fp, key_field="AccessionNumber")


if SELECT_CONTROLS:

    fp = os.path.join(data_root, candidates_fn)
    M = CSVCache(fp, key_field="Accession Number")

    candidates = set()
    controls = set()

    for k, v in M.cache.iteritems():
        d = Dixel(key=k, data=v, dlvl=DLVL.STUDIES, remap_fn=Dixel.remap_montage_keys)
        candidates.add(d)

    experimentals = set()

    for k, v in R.cache.iteritems():
        d = Dixel(key=k, cache=R)
        experimentals.add(d)

    exps = list(experimentals)
    shuffle(exps)

    failed = False
    for item in exps:
        item.data['match_for'] = None

        def best_match(item, age_window):

            def find_best_candidate(age_window):
                for candidate in candidates:
                    if int(item.data['Age']) < int(candidate.data['PatientAge']) + age_window and \
                            int(item.data['Age']) > int(candidate.data['PatientAge']) - age_window:
                        return candidate

            match = find_best_candidate(age_window)

            if match:
                match.data['match_for'] = item.data['AnonAccessionNum']
                item.data['match_for'] = match.data["AccessionNumber"]
                candidates.remove(match)
                controls.add(match)
                logging.info("{} matched to {}".format(int(item.data['Age']), int(match.data['PatientAge'])))
                return True

        for age_window in range(1,15):
            if best_match(item, age_window):
                break
            else:
                logging.error("Could not find +/-{} yr match for {}".format(age_window, item.data['Age']))

        if not item.data.get('match_for'):
            failed = True
            logging.error("Complete match failure")

    logging.info("Match failure: {} w {} controls for {} exps".format( failed, len(controls), len(exps)))

    if not failed:
        fp = os.path.join(data_root, controls_fn)
        N = CSVCache(fp,
                     key_field="AccessionNumber",
                     autosave=False, clear=True)
        for c in controls:
            # print(c.data)
            N.put(key=c.data["AccessionNumber"], data=c.data)
        N.save_fn()
        logging.debug("Saved {} entries".format(len(N)))

if ADD_POST_STUDIES:

    count = 0
    for k,v in R.cache.iteritems():
        if not v.get("Accession # (Post)"):
            continue
        d = Dixel(key=v.get("Accession # (Post)"),
                  data={"AccessionNumber": v.get("Accession # (Post)"),
                        "post_for": v.get("AnonAccessionNum")},
                  cache=R, dlvl=DLVL.STUDIES)
        d.persist()
        logging.debug(d.data)
        count = count + 1
    logging.debug("Found {} post studies".format(count))



if LOOKUP_UIDS:

    proxy = Orthanc(**services[proxy_svc])
    lookup_uids(R, proxy, remote_aet)

    fp = os.path.join(data_root, key_fn)
    create_key_csv(R, fp, key_field="AccessionNumber")


if SET_ANON_IDS:

    set_anon_ids(cache=R)
    for k, v in R.cache.iteritems():
        d = Dixel(key=k, data=v, cache=R)
        d.data['AnonAccessionNum'] = hashlib.md5(d.data["AccessionNumber"]).hexdigest()
        d.data['status'] = 'ready'
        d.persist()

    fp = os.path.join(data_root, key_fn)
    create_key_csv(R, fp, key_field="AccessionNumber")


if COPY_FROM_PACS:

    if not proxy:
        proxy = Orthanc(**services[proxy_svc])
    copy_from_pacs(proxy, remote_aet, R, save_root, depth=1, anon=True)



