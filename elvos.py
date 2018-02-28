"""
Workflow to scrape positives, age match normals, collect, anonymize, and download
"""

import logging
import yaml
import os
from pprint import pformat
from hashlib import md5

from DixelKit import DixelTools
from DixelKit.Montage import Montage
from DixelKit.Orthanc import Orthanc, OrthancProxy
from GUIDMint import PseudoMint

LOOKUP_POS_ANS     = False
MATCH_NORMALS      = False
MERGE_WORKLIST     = False
LOOKUP_SERUIDS     = False
CLEAN_WORKLIST     = False
PULL_FROM_PACS     = False
PUSH_TO_HOUNSFIELD = False
ANONYMIZE_AND_SAVE = False


def lookup_accessions(report_db, csv_fn):
    positives, fieldnames0 = DixelTools.load_csv(csv_fn,
                                                 secondary_id='ReferenceTime')
    logging.debug(positives)
    positives = report_db.update_worklist(positives, time_delta="-1d")

    csv_out = os.path.splitext(csv_fn)[0] + "+mon.csv"
    logging.debug(csv_out)
    DixelTools.save_csv(csv_out, positives)


def match_normals(data_root, pos_csv_fn, neg_csv_fn):

    positives, = DixelTools.load_csv(pos_csv_fn)
    logging.debug(positives)

    candidates, fieldnames = DixelTools.load_csv(neg_csv_fn)
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
    DixelTools.save_csv(csv_out, normals, fieldnames)


def create_worklist(data_root, pos_csv_fn, neg_csv_fn):

    positives, fieldnames0 = DixelTools.load_csv(pos_csv_fn)
    logging.debug(positives)

    normals, fieldnames1 = DixelTools.load_csv(neg_csv_fn)
    logging.debug(normals)

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


def anonymize_and_save(orthanc, data_root, wk_csv_fn, save_dir, noop=True):

    # worklist, fieldnames = DixelTools.load_csv(os.path.join(data_root, wk_csv_fn))
    # logging.debug(worklist)

    worklist = orthanc.studies

    mint = PseudoMint()
    lexicon = {}

    for d in worklist:

        if d not in orthanc.studies:
            logging.warn("Missing study for {}".format(d.meta['Last Name']))
            continue

        d = orthanc.update(d)

        logging.debug(pformat(d.meta))

        name = (d.meta["PatientName"]).upper()
        gender = d.meta['PatientSex']

        if d.meta.get('PatientAge'):
            age = int(d.meta['PatientAge'][0:3])

            new_id = mint.pseudo_identity(name=name,
                                          gender=gender,
                                          age=age)

        elif d.meta.get('PatientBirthDate'):
            dob = d.meta.get('PatientBirthDate')
            dob = dob[:4]+'-'+dob[4:6]+'-'+dob[6:]
            new_id = mint.pseudo_identity(name=name,
                                          gender=gender,
                                          dob=dob)

        logging.debug(pformat(new_id))

        d.meta['AnonID'] = new_id[0]
        d.meta['AnonName'] = new_id[1]
        d.meta['AnonDoB'] = new_id[2].replace('-', '')
        d.meta['AnonAccessionNumber'] = md5(d.meta['AccessionNumber']).hexdigest()

        r = {
            'Remove': ['SeriesNumber'],
            'Replace': {
                'PatientName': d.meta['AnonName'],
                'PatientID': d.meta['AnonID'],
                'PatientBirthDate': d.meta['AnonDoB'],
                'AccessionNumber': d.meta['AnonAccessionNumber']
            },
            'Keep': ['PatientSex', 'StudyDescription', 'SeriesDescription'],
            'Force': True
        }

        logging.debug(pformat(r))

        if os.path.exists(os.path.join(save_dir, d.meta['AnonID']+'.zip')):
            logging.debug('{} already exists -- skipping'.format(d.meta['AnonID']+'.zip'))
            continue

        if not noop:
            e = orthanc.anonymize(d, r)
            e.meta['PatientID'] = d.meta['AnonID']
            logging.debug(d.id)
            logging.debug(e.id)
            orthanc.get(e)
            e.save_archive(save_dir)
            orthanc.delete(e)


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

    data_root = "/Users/derek/Desktop/ELVO"


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

        # cirr1.copy_worklist(hounsfield, worklist, lazy=True)
        deathstar.copy_worklist(hounsfield, worklist, lazy=True)


    if ANONYMIZE_AND_SAVE:
        save_dir = os.path.join(data_root, 'anon')
        anonymize_and_save(hounsfield, data_root, 'worklist+clean.csv', save_dir, noop=False)

    def pull_and_anon_normals(orthanc, data_root, wk_csv_fn, save_dir, noop=False):

        worklist, fieldnames = DixelTools.load_csv(os.path.join(data_root, wk_csv_fn))
        logging.debug(worklist)

        deathstar.get_worklist(worklist, retrieve=True, lazy=True)

        mint = PseudoMint()

        for d in worklist:

            d.level = 'series'
            if not d.meta['RetrieveAETitle'] == "GEPACS":
                logging.debug("Removing {}".format(d.meta['Patient Last Name']))
                worklist_pacs.remove(d)

            continue

            d = orthanc.get(d, retrieve=True, lazy=True)

            logging.debug(pformat(d.meta))

            name = (d.meta["PatientName"]).upper()
            gender = d.meta['PatientSex']

            if d.meta.get('PatientAge'):
                age = int(d.meta['PatientAge'][0:3])

                new_id = mint.pseudo_identity(name=name,
                                              gender=gender,
                                              age=age)

            elif d.meta.get('PatientBirthDate'):
                dob = d.meta.get('PatientBirthDate')
                dob = dob[:4] + '-' + dob[4:6] + '-' + dob[6:]
                new_id = mint.pseudo_identity(name=name,
                                              gender=gender,
                                              dob=dob)

            logging.debug(pformat(new_id))

            d.meta['AnonID'] = new_id[0]
            d.meta['AnonName'] = new_id[1]
            d.meta['AnonDoB'] = new_id[2].replace('-', '')
            d.meta['AnonAccessionNumber'] = md5(d.meta['AccessionNumber']).hexdigest()

            r = {
                'Remove': ['SeriesNumber'],
                'Replace': {
                    'PatientName': d.meta['AnonName'],
                    'PatientID': d.meta['AnonID'],
                    'PatientBirthDate': d.meta['AnonDoB'],
                    'AccessionNumber': d.meta['AnonAccessionNumber']
                },
                'Keep': ['PatientSex', 'StudyDescription', 'SeriesDescription'],
                'Force': True
            }

            logging.debug(pformat(r))

            if os.path.exists(os.path.join(save_dir, d.meta['AnonID'] + '.zip')):
                logging.debug('{} already exists -- skipping'.format(d.meta['AnonID'] + '.zip'))
                continue

            if not noop:
                e = orthanc.anonymize(d, r)
                e.meta['PatientID'] = d.meta['AnonID']
                logging.debug(d.id)
                logging.debug(e.id)
                orthanc.get(e)
                e.save_archive(save_dir)
                orthanc.delete(e)


    PULL_AND_ANON_NORMALS=False
    if PULL_AND_ANON_NORMALS:
        save_dir = "/Volumes/3dlab/elvo_anon/anon"
        pull_and_anon_normals(deathstar, data_root, 'worklist+clean.csv', save_dir, noop=True)


    mint = PseudoMint()
    def get_anon_id(d):

        logging.debug(pformat(d.meta))

        name = (d.meta["PatientName"]).upper()
        gender = d.meta['PatientSex']

        if d.meta.get('PatientAge'):
            age = int(d.meta['PatientAge'][0:3])
            new_id = mint.pseudo_identity(name=name,
                                          gender=gender,
                                          age=age)

        elif d.meta.get('PatientBirthDate'):
            dob = d.meta.get('PatientBirthDate')
            dob = dob[:4] + '-' + dob[4:6] + '-' + dob[6:]
            new_id = mint.pseudo_identity(name=name,
                                          gender=gender,
                                          dob=dob)

        logging.debug(pformat(new_id))

        d.meta['AnonID'] = new_id[0]
        d.meta['AnonName'] = new_id[1]
        d.meta['AnonDoB'] = new_id[2].replace('-', '')
        d.meta['AnonAccessionNumber'] = md5(d.meta['AccessionNumber']).hexdigest()
        return d

    def anonymization_fn(d):

        if not (d.meta.get('AnonName') and d.meta.get('AnonId') and \
                d.meta.get('AnonDoB') and d.meta.get('AnonAccessionNumber') ):
            d = get_anon_id(d)

        r = {
            'Remove': ['SeriesNumber'],
            'Replace': {
                'PatientName': d.meta['AnonName'],
                'PatientID': d.meta['AnonID'],
                'PatientBirthDate': d.meta['AnonDoB'],
                'AccessionNumber': d.meta['AnonAccessionNumber']
            },
            'Keep': ['PatientSex', 'StudyDescription', 'SeriesDescription'],
            'Force': True
        }

        return r



    csv_file = os.path.join(data_root, "worklist+clean.csv")
    worklist, fieldnames = DixelTools.load_csv(csv_file)

    ready_accessions_file = os.path.join(data_root, "ready_positives.txt")
    with open(ready_accessions_file, 'r') as f:
        ready_accessions = f.readlines()

    readylist = set()

    for AccessionNumber in ready_accessions:
        AccessionNumber = AccessionNumber.rstrip()
        logging.debug("Looking for '{}'".format(AccessionNumber))
        for d in worklist:
            # logging.debug("Testing '{}'".format(d.meta['AccessionNumber']))
            if d.meta['AccessionNumber'] == AccessionNumber:
                assert(d.meta["ELVO on CTA?"]=="Yes")
                readylist.add(d)
                worklist.remove(d)
                logging.debug("Found pos {}".format(d))
                break

    matchlist = set()

    for d in readylist:
        for e in worklist:
            if d.meta['Age']==e.meta['Age'] and \
                    d.meta['Gender']==e.meta['Gender'] and \
                    e.meta["ELVO on CTA?"]=="No":
                matchlist.add(e)
                worklist.remove(e)
                logging.debug("Found neg match {} for {}".format(e, d))
                break

    logging.debug(len(readylist))
    logging.debug(len(matchlist))

    # Now we want to do something like lookup all the OIDs for the readylist and figure out
    # their AnonIDs and write them out to spreadsheet

    for d in readylist:
        d = deathstar.update(d)  # Need to get DICOM fields
        d = get_anon_id(d)

    outset = readylist.union(matchlist)
    csv_file = os.path.join(data_root, "ready.csv")
    DixelTools.save_csv(csv_file, outset, fieldnames)

    # Then anonymize each of the readies and write their AnonID out to spreadsheet



