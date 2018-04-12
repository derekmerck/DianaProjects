"""
Anonymize and mirror particular images from hu-christianson to do-christianson

"""
import yaml, logging, os
from hashlib import md5
from DianaFuture import Orthanc, DLVL, RedisCache, Dixel, create_key_csv
from GUIDMint import PseudoMint

mint = PseudoMint()

def set_anon_id(d, ref, lazy=True):
    if not lazy or not \
        (d.data.get('AnonID') and
         d.data.get('AnonName') and
         d.data.get('AnonDoB') ):

        name = ref
        gender = d.data.get('PatientSex', 'M')
        dob = d.data.get('PatientBirthDate')

        if not name or not dob:
            logging.debug('Inadequate data to anonymize {}'.format(d.data.get('PatientID')))
            raise KeyError

        dob = "-".join([dob[:4],dob[4:6],dob[6:]])
        anon = mint.pseudo_identity(name=name, gender=gender, dob=dob)

        d.data['AnonID'] = anon[0]
        d.data['AnonName'] = anon[1]
        d.data['AnonDoB'] = anon[2]

        d.persist()


# ---------------------------------
# SCRIPT
# ---------------------------------

logging.basicConfig(level=logging.DEBUG)

with open("secrets.yml", 'r') as f:
    secrets = yaml.load(f)

INIT_SCACHE = False
INIT_DCACHE = False
CLEAR_DEST = False
SET_ANON_ID = False
CREATE_KEY_CSV = False
COPY_TO_DEST = True

sdb = 9
ddb = 8
data_root = "/Users/derek/Desktop"
key_fn = "morrow_key.csv"
groups_fn = "morrow_groups.yml"

key_fields = [  "AccessionNumber",
                "PatientName",
                "AnonID",
                "AnonName"]


R = RedisCache(db=sdb, clear=INIT_SCACHE)
Q = RedisCache(db=ddb, clear=INIT_DCACHE)

source = Orthanc(**secrets['lifespan']['hounsfield+chrs'])
dest = Orthanc(clear=CLEAR_DEST, **secrets['do']['christianson'])

if INIT_SCACHE:
    src_inv = source.inventory(DLVL.STUDIES)
    for oid in src_inv:
        ret = source.get( Dixel(oid, data={'OID': oid}, dlvl=DLVL.STUDIES) )

        data = {'AccessionNumber':  ret['AccessionNumber'],
                'StudyInstanceUID': ret['StudyInstanceUID'],
                'PatientID':        ret['PatientID'],
                'PatientBirthDate': ret['PatientBirthDate'],
                "PatientName":      ret['PatientName'],
                "PatientSex":       ret['PatientSex'] }

        dixel = Dixel(ret['AccessionNumber'], data=data, dlvl=DLVL.STUDIES)
        assert( dixel.oid() == oid)
        dixel.persist(R)

if INIT_DCACHE:

    dest_inv = dest.inventory(DLVL.STUDIES)
    for oid in dest_inv:
        ret = dest.get( Dixel(oid, data={'OID': oid}, dlvl=DLVL.STUDIES) )

        data = {'AccessionNumber':  ret['AccessionNumber'],
                'StudyInstanceUID': ret['StudyInstanceUID'],
                'PatientID':        ret['PatientID'],
                'PatientBirthDate': ret['PatientBirthDate'],
                "PatientName":      ret['PatientName'],
                "PatientSex":       ret['PatientSex'] }

        dixel = Dixel(ret['AccessionNumber'], data=data, dlvl=DLVL.STUDIES)
        assert( dixel.oid() == oid)
        dixel.persist(Q)

daccs = []
for key in Q.cache.keys():
    d = Dixel(key, cache=Q)
    daccs.append( d.data.get('AccessionNumber') )

logging.debug(daccs)

if SET_ANON_ID:
    with open(os.path.join(data_root, groups_fn), 'r') as f:
        key_data = yaml.load(f)

    logging.debug(key_data)

    for k,v in key_data.iteritems():
        logging.debug(k)
        for vv in v:
            logging.debug("  -> Accession {}".format(vv))
            d = Dixel(vv, cache=R)
            set_anon_id(d, k)
            logging.debug("  -> AnonID {}".format(d.data['AnonID']))

if CREATE_KEY_CSV:
    create_key_csv(R, os.path.join(data_root, key_fn), key_fields=key_fields)

if COPY_TO_DEST:

    for key in R.keys():
        d = Dixel(key=key, cache=R, dlvl=DLVL.STUDIES)

        if d.data.get('AnonID'):
            # One of the chosen, send it

            if md5( d.data.get('AccessionNumber') ).hexdigest() in daccs:
                logging.debug('Found {}/{} ({}) - skipping'.format(d.data.get('PatientName'), d.data.get('AccessionNumber'), d.data.get('AnonName')))
                continue
            else:
                logging.debug('Working on {}/{} ({})'.format(d.data.get('PatientName'), d.data.get('AccessionNumber'), d.data.get('AnonName')))

            if not d.data.get('AnonOID'):
                r = source.anonymize(d)
                logging.debug(r)
                d.data['AnonOID'] = r['ID']
                d.persist()

            # Need an oid and an anon name to save...
            e = Dixel(key=d.data['AnonOID'],
                      data={'OID': d.data['AnonOID'],
                            'PatientID': d.data['AnonID']},
                      dlvl=d.dlvl)

            # logging.debug(e.oid())

            if e not in dest:
                source.copy(e, dest)
            if e in source:
                source.remove(e)