import logging
import yaml
import os
from DixelKit import DixelTools
from DixelKit.Montage import Montage

if __name__ == "__main__":

    logging.basicConfig(level=logging.DEBUG)
    with open("secrets.yml", 'r') as f:
        secrets = yaml.load(f)

    # 1. Start from GI section MRN + RefTime list
    # ------------------------------------
    csv_file = "/Users/derek/Projects/GI Bleeds/lgib_full.csv"

    worklist, fieldnames = DixelTools.load_csv(csv_file, secondary_id='ReferenceTime')
    logging.debug(worklist)

    # for i in xrange(1, 1200):
    #     worklist.pop()

    # 2. Look in Montage for report info/Accession Numbers
    # ------------------------------------

    montage = Montage(**secrets['services']['montage'])

    # montage.add_exam_codes({'IR657':  7855,   # Vascular selection, other codes?
    #                         'IMG240': 7797,   # CTA Ab/Pelvis
    #                         'IMG794': 7740,   # CT Ab/Pelvis W IV
    #                         'IMG5400': 8874   # CTA ENDOLEAK
    #                         })

    qdict = { "exam_type":  [7740, 8874]}
    worklist = montage.update_worklist(worklist, time_delta="+1d", qdict=qdict, suffix="+CT")

    # Catches some 239s and 216s
    qdict = { "cpt": [10913, 10912, 10824]}
    worklist = montage.update_worklist(worklist, time_delta="+1d", qdict=qdict, suffix="+CT")

    qdict = { "exam_type":  [7855]}
    worklist = montage.update_worklist(worklist, time_delta="+1d", qdict=qdict, suffix="+XA")

    # Reorganize fieldnames

    fieldnames.insert(1, 'First Name')
    fieldnames.insert(2, 'Last Name')
    fieldnames.insert(3, 'Age')

    fieldnames.append('AccessionNumber+CT')
    fieldnames.append('MID+CT')
    fieldnames.append('ExamCode+CT')
    fieldnames.append('Report+CT')
    fieldnames.append('ExamCompleted+CT')

    fieldnames.append('AccessionNumber+XA')
    fieldnames.append('MID+XA')
    fieldnames.append('ExamCode+XA')
    fieldnames.append('Report+XA')
    fieldnames.append('ExamCompleted+XA')

    csv_out = os.path.splitext(csv_file)[0]+"+mon2.csv"
    DixelTools.save_csv(csv_out, worklist, fieldnames)


