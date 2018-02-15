from DixelKit import DixelTools
from DixelKit.Dixel import Dixel
from DixelKit.Orthanc import OrthancProxy

import logging
import os
import yaml


def lookup_seruids(data_root, csv_fn):

    csv_in = os.path.join(data_root, csv_fn)
    worklist, fieldnames = DixelTools.load_csv(csv_in)

    qdict = {'SeriesDescription': 'nc*renal stone'}
    worklist = deathstar.update_worklist(worklist, qdict=qdict, suffix='+rs')

    qdict = {'SeriesDescription': '*low dose renal*'}
    worklist = deathstar.update_worklist(worklist, qdict=qdict, suffix='+uld')

    csv_out = os.path.splitext(csv_fn)[0]+"+seruids.csv"
    DixelTools.save_csv(os.path.join(data_root, csv_out), worklist, fieldnames)

    return worklist


if __name__=="__main__":

    logging.basicConfig(level=logging.DEBUG)
    with open("secrets.yml", 'r') as f:
        secrets = yaml.load(f)

    deathstar = OrthancProxy(**secrets['services']['deathstar'])

    data_root = "/Users/derek/Desktop/uldrs"
    csv_fn = "uldrs2.csv"

    # worklist = consolidate_worklists(data_root, csv_fn)
    # lookup_seruids(data_root, csv_fn)

    csv_fn = "uldrs2+seruids.csv"
    csv_in = os.path.join(data_root, csv_fn)
    worklist_, _ = DixelTools.load_csv(csv_in)

    worklist = set()

    for d in worklist_:

        meta1 = d.meta
        meta1['StudyInstanceUID'] = d.meta['StudyInstanceUID+uld']
        meta1['SeriesInstanceUID'] = d.meta['SeriesInstanceUID+uld']
        meta1['OID'] = DixelTools.orthanc_id(d.meta['PatientID'],
                                             meta1['StudyInstanceUID'],
                                             meta1['SeriesInstanceUID'])
        logging.debug(meta1['OID'])

        worklist.add(Dixel(id=meta1['OID'], meta=meta1, level='series'))

        meta2 = d.meta
        meta2['StudyInstanceUID'] = d.meta['StudyInstanceUID+rs']
        meta2['SeriesInstanceUID'] = d.meta['SeriesInstanceUID+rs']
        meta2['OID'] = DixelTools.orthanc_id(d.meta['PatientID'],
                                             meta2['StudyInstanceUID'],
                                             meta2['SeriesInstanceUID'])
        logging.debug(meta2['OID'])

        worklist.add(Dixel(id=meta2['OID'], meta=meta2, level='series'))

    logging.debug(worklist)

    deathstar.get_worklist(worklist, retrieve=True, lazy=True)
