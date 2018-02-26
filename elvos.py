import logging
import yaml
import os
from pprint import pformat
from hashlib import md5

from DixelKit import DixelTools
from DixelKit.Montage import Montage
from DixelKit.Orthanc import Orthanc, OrthancProxy
from utilities.GUIDMint.GUIDMint import PseudoMint

LOOKUP_POS_ANS     = False
MATCH_NORMALS      = False
MERGE_WORKLIST     = False
LOOKUP_SERUIDS     = False
CLEAN_WORKLIST     = False
PULL_FROM_PACS     = False
PUSH_TO_HOUNSFIELD = False
ANONYMIZE_AND_SAVE = True


def lookup_accessions(report_db, csv_fn):
    positives, fieldnames0 = DixelTools.load_csv(csv_fn,
                                                 secondary_id='ReferenceTime')
    logging.debug(positives)
    positives = report_db.update_worklist(positives, time_delta="-1d")

    csv_out = os.path.splitext(csv_fn)[0] + "+mon.csv"
    logging.debug(csv_out)
    DixelTools.save_csv(csv_out, positives)


def match_normals(data_root, pos_csv_fn, neg_csv_fn):

    candidates, fieldnames1 = DixelTools.load_csv(neg_csv_fn)
    logging.debug(candidates)

    # Remove any f/u scans from same patient to prevent confusion
    logging.debug("Before rem pos: {}".format(len(candidates)))
    pos_mrns = {d.meta['PatientID'] for d in positives}
    candidates = {d for d in candidates if d.meta['PatientID'] not in pos_mrns}
    logging.debug("After  rem pos: {}".format(len(candidates)))

    # Select age-matched normal/negative for each positive
    normals = set()
    for item in positives:

        def compare(candidate):
            return candidate.meta['Age'] == item.meta['Age'] and \
                   candidate.meta['Gender'] == item.meta['Gender']


        ds = {d for d in candidates if compare(d)}
        d = ds.pop()

        if d:
            candidates.remove(d)
            normals.add(d)
            logging.debug("Found {}:{}/{:6} match for {}:{}/{}".format(
                d.meta['PatientID'][-5:-1],
                d.meta['Age'], d.meta['Gender'],
                item.meta['PatientID'][-5:-1],
                item.meta['Age'], item.meta['Gender']))
        else:
            raise Exception("No age/sex match found for {}/{}".format(item.meta['Age'], item.meta['Gender']))

    logging.debug(normals)  # Remove any f/u scans from same patient to prevent confusion
    logging.debug("After pos matching: {} (compare {})".format(len(normals),
                                                               len(positives)))

    csv_out = os.path.splitext(neg_csv_file)[0] + "+matched.csv"
    logging.debug(csv_out)
    DixelTools.save_csv(csv_out, normals, fieldnames1)


def create_worklist(data_root, pos_csv_fn, neg_csv_fn):
    for d in positives:
        d.meta['categories'] = ['positive', 'head', 'cta']
    for d in normals:
        d.meta['ELVO on CTA?'] = "No"
        d.meta['categories'] = ['normal', 'head', 'cta']

    worklist = positives.union(normals)

    logging.debug("After merging: {}".format(len(worklist)))

    # Merge fieldnames but keep order as much as possible
    for f in fieldnames1:
        if f not in fieldnames0:
            fieldnames0.append(f)

    csv_file = "/Users/derek/Desktop/ELVO/worklist.csv"
    DixelTools.save_csv(csv_file, worklist, fieldnames0)


def anonymize_and_save(orthanc, save_dir, noop=True):

    mint = PseudoMint()
    lexicon = {}

    for d in orthanc.series:
        d = orthanc.update(d)

        logging.debug(pformat(d.meta))

        name = d.meta["PatientName"]
        gender = d.meta['PatientSex']
        age = int(d.meta['PatientAge'][0:3])

        new_id = mint.pseudo_identity(name=name,
                                      gender=gender,
                                      age=age)

        logging.debug(pformat(new_id))

        lexicon[d.meta['PatientName'],d.meta['PatientID']] = new_id

        r = {
            'Remove': ['SeriesNumber'],
            'Replace': {
                'PatientName': new_id[1],
                'PatientID': new_id[0],
                'PatientBirthDate': new_id[2].replace('-', ''),
                'AccessionNumber': md5(d.meta['AccessionNumber']).hexdigest()
            },
            'Keep': ['PatientSex', 'StudyDescription', 'SeriesDescription'],
            'Force': True
        }

        logging.debug(pformat(r))

        if not noop:
            e = orthanc.anonymize(d, r)
            logging.debug(d.id)
            logging.debug(e.id)
        #     orthanc.save(e, save_dir)
        #     orthanc.delete(e)

    logging.debug(pformat(lexicon))
    return lexicon



if __name__ == "__main__":

    logging.basicConfig(level=logging.DEBUG)

    # Setup Gateways
    # ------------------------------------
    with open("secrets.yml", 'r') as f:
        secrets = yaml.load(f)
    deathstar = OrthancProxy(**secrets['services']['deathstar'])
    # cirr1 = Orthanc(**secrets['services']['hounsfield+elvo'])
    hounsfield = Orthanc(**secrets['services']['hounsfield+elvo'])
    montage = Montage(**secrets['services']['montage'])

    data_root = "/Users/derek/Desktop/ELVOs"


    # 1. Look in Montage for report info/Accession Numbers for positives
    # ------------------------------------
    if LOOKUP_POS_ANS:
        # MRN's + date of service
        pos_csv_fn = "elvos4.csv"
        lookup_accessions(montage, data_root, pos_csv_fn)


    # 2. Select normals from candidate pools
    # -------------------------------------
    if MATCH_NORMALS:
        # Montage data, already has accessions
        pos_csv_fn = "elvos4+mon.csv"
        neg_csv_file = "normals.csv"
        match_normals(data_root, pos_csv_fn, neg_csv_file)


    # 3. Categorize the dixels and merge datasets
    # ------------------------------------
    if MERGE_WORKLIST:
        # Montage data, already has accessions
        pos_csv_fn = "elvos4+mon.csv"
        neg_csv_file = "normals+matched.csv"
        create_worklist(data_root, pos_csv_fn, neg_csv_file)


    # 4. Find items on the CIRR and mark RetreiveFrom as peer
    # ------------------------------------
    # CIRR down!


    # 5. Find items on the CIRR or PACS proxy and mark RetreiveFrom as modality
    # ------------------------------------
    if LOOKUP_SERUIDS:

        csv_file = "/Users/derek/Desktop/ELVO/worklist.csv"
        worklist, fieldnames = DixelTools.load_csv(csv_file)

        deathstar = OrthancProxy(**secrets['services']['deathstar'])

        qdict = {'SeriesDescription': '*axial*brain*cta*'}
        worklist = deathstar.update_worklist(worklist, qdict=qdict)

        qdict = {'SeriesDescription': '*brain*cta*'}
        worklist = deathstar.update_worklist(worklist, qdict=qdict)

        qdict = {'SeriesDescription': '*arterial*thin*axial'}
        worklist = deathstar.update_worklist(worklist, qdict=qdict)

        qdict = {'SeriesDescription': 'CTA Head and Neck'}
        worklist = deathstar.update_worklist(worklist, qdict=qdict)

        qdict = {'SeriesDescription': 'arterial axial reformat'}
        worklist = deathstar.update_worklist(worklist, qdict=qdict)

        csv_out = os.path.splitext(csv_file)[0]+"+seruids.csv"
        logging.debug(csv_out)
        DixelTools.save_csv(csv_out, worklist, fieldnames)


    # 4. Copy data from relevant sources
    # ------------------------------------

    if CLEAN_WORKLIST:

        csv_file = "/Users/derek/Desktop/ELVO/worklist+seruids.csv"
        worklist, fieldnames = DixelTools.load_csv(csv_file)

        logging.debug(fieldnames)

        # Clean up the worklist a little bit

        for d in worklist:

            if d.meta.get('Patient First Name'):
                d.meta['First Name'] = d.meta['Patient First Name']
            if d.meta.get('Patient Last Name'):
                d.meta['Last Name'] = d.meta['Patient Last Name']

            if not d.meta.get('OID') and \
                   d.meta.get('StudyInstanceUID') and \
                   d.meta.get('SeriesInstanceUID'):

                d.meta['OID'] = DixelTools.orthanc_id(d.meta['PatientID'],
                                                      d.meta['StudyInstanceUID'],
                                                      d.meta['SeriesInstanceUID'])
                d.id = d.meta['OID']

            del (d.meta['SpecificCharacterSet'])
            del (d.meta['Patient First Name'])
            del (d.meta['Patient Last Name'])


        fieldnames.remove('Patient First Name')
        fieldnames.remove('Patient Last Name')
        fieldnames.remove('SpecificCharacterSet')

        csv_file = "/Users/derek/Desktop/ELVO/worklist+clean.csv"
        DixelTools.save_csv(csv_file, worklist, fieldnames)

    if PULL_FROM_PACS:

        csv_file = "/Users/derek/Desktop/ELVO/worklist+clean.csv"
        worklist, fieldnames = DixelTools.load_csv(csv_file)

        worklist_pacs = set()
        worklist_cirr = set()
        for d in worklist:
            d.level = 'series'
            if d.meta['RetrieveAETitle'] == "GEPACS":
                worklist_pacs.add(d)
            else:
                worklist_cirr.add(d)

        logging.debug("Found {} series to pull from PACS".format(len(worklist_pacs)))

        deathstar.get_worklist(worklist_pacs, retrieve=True, lazy=True)

        cirr1.copy_worklist(hounsfield, worklist, lazy=True)
        deathstar.copy_worklist(hounsfield, worklist, lazy=True)


    if ANONYMIZE_AND_SAVE:
        anonymize_and_save(hounsfield, '/tmp', noop=False)