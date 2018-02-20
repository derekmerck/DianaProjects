
from DixelKit import DixelTools
from DixelKit.Orthanc import OrthancProxy

import logging
import os
import yaml

def consolidate_worklists(data_root, csv_out):
    # Montage csv dump of 4s and 5s with UNKNOWN status
    birads45u_file = "birads_4_5+unk.csv"

    # Montage csv dump of 4s and 5s with KNOWN POSITIVE status
    # ("BI-RADS CATEGORY 4" | "BI-RADS CATEGORY 5") & ("Recently diagnosed" | "Newly diagnosed" | "New diagnosis")
    birads45p_file = "birads_4_5+pos.csv"

    # Montage csv dump of 4s and 5s with KNOWN POSITIVE status
    birads6p_file = "birads_6+pos.csv"

    # Build all our positives
    b45p, fieldnames = DixelTools.load_csv(os.path.join(data_root, birads45p_file))
    b6p, fieldnames = DixelTools.load_csv(os.path.join(data_root, birads6p_file))
    pos = b45p.union(b6p)

    b45u, fieldnames = DixelTools.load_csv(os.path.join(data_root, birads45u_file))

    unk = b45u.difference(pos)

    logging.debug("size b45u: {}".format(len(b45u)))
    logging.debug("size b45p: {}".format(len(b45p)))
    logging.debug("size b6p: {}".format(len(b6p)))

    logging.debug("size pos: {}".format(len(pos)))
    logging.debug("size unk: {}".format(len(unk)))

    for d in pos:
        d.meta['Cancer Status'] = "Positive"

    for d in unk:
        d.meta['Cancer Status'] = "Unknown"

    all = pos.union(unk)

    logging.debug("size all: {}".format(len(all)))

    keep = ['AccessionNumber', 'PatientID', 'Report Text', 'Exam Code', 'Patient Age', 'Cancer Status']
    for d in all:
        for m in d.meta.keys():
            if m not in keep:
                del(d.meta[m])

    DixelTools.save_csv(os.path.join(data_root, csv_out), all, keep)


def lookup_seruids(data_root, csv_fn):

    csv_in = os.path.join(data_root, csv_fn)
    worklist, fieldnames = DixelTools.load_csv(csv_in)

    deathstar = OrthancProxy(**secrets['services']['deathstar'])

    qdict = {'SeriesDescription': '*STIR*'}
    worklist = deathstar.update_worklist(worklist, qdict=qdict, suffix='+STIR')

    qdict = {'SeriesDescription': '1*MIN*SUB*'}
    worklist = deathstar.update_worklist(worklist, qdict=qdict, suffix='+1MS')

    qdict = {'SeriesDescription': '2*MIN*SUB*'}
    worklist = deathstar.update_worklist(worklist, qdict=qdict, suffix='+2MS')

    qdict = {'SeriesDescription': '6*MIN*SUB*'}
    worklist = deathstar.update_worklist(worklist, qdict=qdict, suffix='+6MS')

    csv_out = os.path.splitext(csv_fn)[0]+"+seruids.csv"
    DixelTools.save_csv(os.path.join(data_root, csv_out), worklist, fieldnames)


if __name__=="__main__":

    logging.basicConfig(level=logging.DEBUG)
    with open("secrets.yml", 'r') as f:
        secrets = yaml.load(f)

    data_root = "/Users/derek/Desktop/breast mr ml/bi-rads"
    csv_fn = "all_candidates.csv"

    # worklist = consolidate_worklists(data_root, csv_fn)
    lookup_seruids(data_root, csv_fn)


