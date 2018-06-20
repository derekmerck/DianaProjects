import glob, os, logging, hashlib
from DianaFuture import RedisCache, CSVCache, Dixel, DLVL
from DixelKit import Report

logging.basicConfig(level=logging.DEBUG)

data_dir = "/Users/derek/data/RADCAT/ED QI"

out_fn = "ed_qi_anon+phi.csv"
out_fp = os.path.join(data_dir, out_fn)

csv_files = glob.glob( os.path.join( data_dir, "ct_*.csv") )

from GUIDMint import PseudoMint

worklist = set()

mint = PseudoMint()

for file in csv_files:

    M = CSVCache(file, "Accession Number")
    for k,v in M.cache.iteritems():
        d = Dixel(key=k, data=v, remap_fn=Dixel.remap_montage_keys)
        fn = os.path.split(file)[1]
        base_fn = os.path.splitext(fn)[0]
        d.data['Query'] = base_fn
        worklist.add(d)

N = CSVCache(out_fp,
             key_field="AccessionNumber",
             autosave=False,
             clear=True)

duplicates = 0

def anonymize(d):

    name = "{}^{}".format(d.data['PatientFirstName'].upper(), d.data['PatientLastName'].upper())
    gender = d.data["PatientSex"]
    age = d.data["PatientAge"]

    secret = "|".join([name, gender, age])

    guid = mint.mint_guid(secret)
    pseudonym = mint.pseudonym(guid, gender)

    d.data['AnonPatientID'] = guid
    d.data['AnonPatientName'] = pseudonym
    d.data['AnonAccessionNumber'] = hashlib.md5(d.data["AccessionNumber"]).hexdigest()

    refp_guid = mint.mint_guid(d.data['ReferringPhysicianName'])
    refp_pname = mint.pseudonym(refp_guid)

    d.data['AnonReferringPhysicianName'] = refp_pname


for d in worklist:

    r = Report(d.data['ReportText'])
    try:
        ext = r.extractions()
        anon = r.anonymized()
    except:
        logging.error('Bad Report Text')
        logging.error(d.data['ReportText'])
        logging.error(d.data['AccessionNumber'])
        logging.error(d.data['Query'])
        ext = {}
        anon = ''
    if not ext.get('radcat') or not anon:
        continue
    d.data['radcat'] = ext.get('radcat', '')
    d.data['radcat3'] = ext.get('radcat3', '')

    d.data["ReportText"] = anon
    anonymize(d)

    if d.key in N.keys():
        duplicates += 1
        logging.warn("Found duplicate accession number in two queries:")
        logging.warn(d.data['Query'])
        logging.warn(N.get(d.key)['Query'])

    N.put(d.key, d.data)

if duplicates:
    logging.warn("Found {} duplicates out of {} entries".format(duplicates, len(worklist)))

fieldnames = [

    'Organization',
    'PatientStatus',
    'OrderCode',

    'AccessionNumber',
    'PatientID',
    'PatientAge',
    'PatientSex',

    'AnonAccessionNumber',
    'AnonPatientID',
    'AnonPatientName',

    'StudyDate',
    'StudyDescription',
    'ReferringPhysicianName',

    'AnonReferringPhysicianName',

    'ReportText',

    'Query',
    'radcat',
    'radcat3'
    ]


N.save_fn(fieldnames=fieldnames)



