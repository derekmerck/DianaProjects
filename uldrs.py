"""
Grand/Laurie Low-Dose Renal Stone Research Protocol

Merck, Winter 2018

1. Start w accession nums/mrns from Montage
2. Lookup seruids for each study type
3. Copy series from the PACS to the proxy (deathstar)
4. Copy series from the proxy (deathstar) to the archive (hounsfield)
5. Anonymize each series and delete the original

## Create a docker repo on Hounsfield

docker run -p 4251:4242 -p 8051:8042 --rm -d -v /tmp/orthanc_uldrs.json:/etc/orthanc/orthanc.json:ro --name uldrs osimis/orthanc --env WVP_ALPHA_ENABLED=true
"""

from DixelKit import DixelTools
from DixelKit.Dixel import Dixel
from DixelKit.Orthanc import Orthanc, OrthancProxy
from utilities.GUIDMint.GUIDMint import PseudoMint

from pprint import pformat
import logging
import os
import yaml
from hashlib import md5


def lookup_seruids(proxy, series_qs, data_root, csv_fn):

    csv_in = os.path.join(data_root, csv_fn)
    worklist, fieldnames = DixelTools.load_csv(csv_in)

    for item in series_qs:

        qdict = item['qdict']
        worklist = proxy.update_worklist(worklist, qdict=qdict, suffix=item['suffix'])

    csv_out = os.path.splitext(csv_fn)[0]+"+seruids.csv"
    DixelTools.save_csv(os.path.join(data_root, csv_out), worklist, fieldnames)

    return worklist


def series_dixel(d, suffix=""):

    meta1 = {}
    meta1['PatientID']         = d.meta['PatientID']
    meta1['AccessionNumber']   = d.meta['AccessionNumber'+suffix]
    meta1['StudyInstanceUID']  = d.meta['StudyInstanceUID'+suffix]
    meta1['SeriesInstanceUID'] = d.meta['SeriesInstanceUID'+suffix]
    meta1['OID'] = DixelTools.orthanc_id(meta1['PatientID'],
                                         meta1['StudyInstanceUID'],
                                         meta1['SeriesInstanceUID'])
    logging.debug(meta1['OID'])
    return Dixel(id=meta1['OID'], meta=meta1, level='series')


def copy_from_pacs(proxy, data_root, csv_fn ):
    csv_in = os.path.join(data_root, csv_fn)
    worklist_, _ = DixelTools.load_csv(csv_in)

    worklist = set()

    # Split studies with multiple series
    for d in worklist_:
        d0 = series_dixel(d, suffix="+uld")
        d1 = series_dixel(d, suffix="+rs")
        worklist.add(d0)
        worklist.add(d1)

    logging.debug(worklist)

    proxy.get_worklist(worklist, retrieve=True, lazy=True)


def anonymize_and_delete(orthanc, noop=False):
    logging.debug(orthanc.series)
    mint = PseudoMint()
    lexicon = {}

    for d in orthanc.series:

        d = orthanc.update(d)

        # # Delete anonymized data
        # if d.meta.get('Anonymized'):
        #     hounsfield.delete(d)
        #     continue

        # logging.debug(pformat(d.meta))

        name = d.meta["PatientName"] + d.meta['SeriesNumber']
        gender = d.meta['PatientSex']
        age = int(d.meta['PatientAge'][0:3]) + int(d.meta['SeriesNumber'])

        new_id = mint.pseudo_identity(name=name,
                                      gender=gender,
                                      age=age)

        logging.debug(pformat(new_id))

        if d.meta['PatientName'] not in lexicon.keys():
            lexicon[d.meta['PatientName']] = {}
        lexicon[d.meta['PatientName']][d.meta['SeriesDescription']] = new_id

        r = {
            'Remove': ['SeriesNumber'],
            'Replace': {
                'PatientName': new_id[1],
                'PatientID': new_id[0],
                'PatientBirthDate': new_id[2].replace('-', ''),
                'AccessionNumber': md5(d.meta['AccessionNumber']).hexdigest(),
                'StudyDescription': "Ultra-Low Dose Renal Stone Research",
                'SeriesDescription': "Blinded Series"
            },
            'Keep': ['PatientSex'],
            'Force': True
        }

        # logging.debug(pformat(r))

        if not noop:
            orthanc.anonymize(d, r)
            orthanc.delete(d)

    logging.debug(pformat(lexicon))
    return lexicon


if __name__=="__main__":

    logging.basicConfig(level=logging.DEBUG)
    with open("secrets.yml", 'r') as f:
        secrets = yaml.load(f)

    deathstar = OrthancProxy(**secrets['services']['deathstar'])
    hounsfield = Orthanc(**secrets['services']['hounsfield+uldrs'])

    data_root = "/Users/derek/Desktop/uldrs"
    csv_fn = "uldrs2.csv"

    # Get SERUIDS
    series_qs = [
        {'qdict': {'SeriesDescription': 'nc*renal stone'},
         'suffix': "+rs"},
        {'qdict': {'SeriesDescription': '*low dose renal*'},
         'suffix': "+uld"}
    ]
    # lookup_seruids(deathstar, series_qs, data_root, csv_fn)

    # Copy from PACS
    csv_fn = "uldrs2+seruids.csv"
    # copy_from_pacs(deathstar, data_root, csv_fn)

    # Copy to Hounsfield
    # deathstar.copy_inventory(hounsfield)

    # Anonymize and delete source
    lexicon = anonymize_and_delete(hounsfield, noop=True)

    # Do something with the lexicon...

