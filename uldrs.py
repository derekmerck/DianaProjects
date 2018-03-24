"""
Grand/Laurie Low-Dose Renal Stone Research Protocol
Merck, Winter 2018

Status: Done

20 subjects with paired "low dose renal stone" and "ultra-low dose renal stone" protocols
42 individual CT series, each subject _split_ into 2 pseudo-ids

## Create a docker Osimis Viewer repo on Hounsfield

```bash
$ docker run -p 4251:4242 -p 8051:8042 --rm -d -v /tmp/orthanc_uldrs.json:/etc/orthanc/orthanc.json:ro --name uldrs osimis/orthanc --env WVP_ALPHA_ENABLED=true
```
"""

from pprint import pformat
import logging
import os
import yaml
from hashlib import md5

from DixelKit import DixelTools
from DixelKit.Dixel import Dixel, DicomLevel
from DixelKit.Orthanc import Orthanc, OrthancProxy
from GUIDMint.GUIDMint import PseudoMint
from DixelKit.DixelReader import DixelReader, MetaTranslator

data_root = "/Users/derek/Projects/Body/CT Ultralow Dose Renal Stone"
csv_fn = "uldrs3.csv"

def anon_fn(d):
    return {
        'Remove': ['SeriesNumber'],
        'Replace': {
            'PatientName': d.meta['AnonName'],
            'PatientID': d.meta['AnonID'],
            'PatientBirthDate': d.meta['AnonDoB'].replace('-', ''),
            'AccessionNumber': md5(d.meta['AccessionNumber']).hexdigest(),
            'StudyDescription': "Ultra-Low Dose Renal Stone Research",
            'SeriesDescription': "Blinded Series"
        },
        'Keep': ['PatientSex', 'StudyInstanceUID', 'SeriesInstanceUID', 'SOPInstanceUID'],
        'Force': True
    }


def anonymize(data_root, csv_fn):

    mint = PseudoMint()
    worklist, fieldnames = DixelTools.load_csv(os.path.join(data_root, csv_fn), dicom_level=DicomLevel.SERIES)

    for d in worklist:

        if "uldrs" in d.meta['tags']:
            study_type = "uldrs"
            age_offset = int(d.meta['AccessionNumber'][-3:-2])/2
        elif "ncrs" in d.meta['tags']:
            study_type = "ncrs"
            age_offset = -int(d.meta['AccessionNumber'][-2:-1])/2
        else:
            logging.warn("No study type tag!")
            study_type = 'None'

        name = d.meta["AccessionNumber"] + study_type
        gender = d.meta.get('PatientSex')

        from dateutil.relativedelta import relativedelta
        from datetime import date, datetime, timedelta
        import time

        def calculate_age(born):
            today = date.today()
            return today.year - born.year - ((today.month, today.day) < (born.month, born.day))

        dob = datetime( *time.strptime(d.meta['PatientBirthDate'], "%Y%m%d")[0:6] )
        age = calculate_age(dob + relativedelta(months=age_offset) )

        new_id = mint.pseudo_identity(name=name,
                                      gender=gender,
                                      age=age)

        d.meta['AnonID']   = new_id[0]
        d.meta['AnonName'] = new_id[1]
        d.meta['AnonDoB']  = new_id[2]
        d.meta['AnonAge']  = age

    csv_out = '{}+anon{}'.format(os.path.splitext(csv_fn)[0], os.path.splitext(csv_fn)[1])
    DixelTools.save_csv(os.path.join(data_root, csv_out), worklist, fieldnames)


def copy_from_pacs(proxy, data_root, csv_fn,
                   dest=None, anon_fn=anon_fn,
                   keep_anon=False):

    csv_file = os.path.join(data_root, csv_fn)
    worklist, fieldnames = DixelTools.load_csv(csv_file, dicom_level=DicomLevel.SERIES)

    for d in worklist:

        if not d.meta.get("AnonID") or \
                not d.meta.get("AnonDoB") or \
                not d.meta.get("AnonName"):
            logging.warn("Incomplete anonymization for {}".format(d))
            continue

        e_ = Dixel(id=d.meta.get('AnonOID'), level=DicomLevel.SERIES)
        if e_ not in dest.series:
            logging.warn("Missing patient {} on HOUNSFIELD -- Pulling on DEATHSTAR".format(d))

            proxy.get(d, retrieve=True, lazy=True)
            if not d.meta.get('OID'):
                logging.warn("Can't figure out OID for {}, apparently not retrieved".format(d))

            e = proxy.anonymize(d, anon_fn(d))
            e = proxy.update(e)
            logging.debug(pformat(e.meta))
            d.meta['AnonOID'] = e.id

            csv_out = '{}+incr{}'.format(os.path.splitext(csv_fn)[0], os.path.splitext(csv_fn)[1])
            DixelTools.save_csv(os.path.join(data_root, csv_out), worklist, fieldnames)

            proxy.copy(e, dest)

        if not keep_anon:
            proxy.delete(e)



if __name__=="__main__":

    logging.basicConfig(level=logging.DEBUG)
    with open("secrets.yml", 'r') as f:
        secrets = yaml.load(f)

    deathstar = OrthancProxy(**secrets['services']['deathstar'])
    hounsfield = Orthanc(**secrets['services']['hounsfield+uldrs'])

    reader = DixelReader(os.path.join(data_root, csv_fn))
    tmp = reader.read_csv2()

    worklist = set()
    for w in tmp:
        m = MetaTranslator.translate_meta(w)
        d = Dixel(id=''.join(m['id']), meta=m, level=DicomLevel.SERIES)
        worklist.add(d)
        # logging.debug(d.meta)

    # Get SERUIDS
    series_qs = [{'qdict': {'SeriesInstanceUID': ''}}]
    DixelTools.lookup_seruids(deathstar, series_qs,
                              worklist=worklist,
                              data_root=data_root, csv_fn=csv_fn,
                              save_file=True)

    # Create Anon tags
    csv_fn = "uldrs3+seruids.csv"
    anonymize(data_root, csv_fn)

    # Copy from PACS
    csv_fn = "uldrs3+seruids+anon.csv"
    copy_from_pacs(deathstar, data_root, csv_fn,
                   dest=hounsfield, anon_fn=anon_fn)


    import itertools
    def create_metadata(data_root, csv_fn):
        csv_file = os.path.join(data_root, csv_fn)
        worklist, fieldnames = DixelTools.load_csv(csv_file, dicom_level=DicomLevel.SERIES)

        fieldnames = [ 'PatientID', 'PatientName', 'PatientBirthDate',
                       'AnonID', 'AnonName', 'AnonAge', 'AnonDoB', 'PatientSex' ]

        def keyfunc(v):
            return [v.meta[k] for k in fieldnames]
        groups = []
        worklist_ = sorted(list(worklist), key=keyfunc)
        for k, g in itertools.groupby(worklist_, keyfunc):
            groups.append(list(g))      # Store group iterator as a list

        logging.debug(groups)

        worklist = set()
        for g in groups:
            worklist.add(g[0])


        csv_out = '{}+key{}'.format(os.path.splitext(csv_fn)[0], os.path.splitext(csv_fn)[1])
        DixelTools.save_csv(os.path.join(data_root, csv_out), worklist, fieldnames, extras=False)

        for d in worklist:
            d.meta['PatientID'] = d.meta['AnonID']
            d.meta['PatientName'] = d.meta['AnonName']
            d.meta['PatientDoB'] = d.meta['AnonDoB']
            d.meta['PatientAge'] = d.meta['AnonAge']

        fieldnames = [ 'PatientID', 'PatientName', 'PatientBirthDate','PatientSex' ]

        csv_out = '{}+info{}'.format(os.path.splitext(csv_fn)[0], os.path.splitext(csv_fn)[1])
        DixelTools.save_csv(os.path.join(data_root, csv_out), worklist, fieldnames, extras=False)

    # Create key
    csv_fn = "uldrs3+seruids+anon.csv"
    create_metadata(data_root, csv_fn)


