"""
LCSR Data Extracts
Merck, Winter 2018

Status: Done

Extract all possible lung screening registry data from Montage dumps
"""

import logging
import os
import csv
from pprint import pformat

from DixelKit import DixelTools
from Report import Report

DATA_ROOT = "/Users/derek/Projects/lscr"

def partial_key(d, pk):
    for k in d.keys():
        if pk.lower() in k.lower():
            return d[k]


if __name__ == "__main__":

    logging.basicConfig(level=logging.DEBUG)

    # Starting from a montage csv dump of all patients
    # with IMG8119 (lscr) and IMG1997 (lscr f/u)
    csv_file = os.path.join(DATA_ROOT, "lscr_both.csv")
    npi_file = os.path.join(DATA_ROOT, "rad2npi.csv" )

    worklist, fieldnames = DixelTools.load_csv(csv_file)
    logging.debug(worklist)

    rad2npi = {}
    with open(npi_file, 'rU') as f:
        items = csv.DictReader(f)
        for item in items:
            logging.debug(item)
            rad2npi[item['Radiologist']] = item['NPI']

    logging.debug(pformat(rad2npi))

    for d in worklist:

        r = Report(d.meta['Report Text'])

        r = Report(d)  # Automatically populates d with extractions
        d.meta['oph_last']  = d.meta['Ordered By'].split(', ')[0].capitalize()
        d.meta['oph_first'] = d.meta['Ordered By'].split(', ')[1].capitalize()

        rad = d.meta['Report Finalized By'].split(',')[0]
        logging.debug("OB: {} {} / RB: {}".format(d.meta['oph_first'],
                                                  d.meta['oph_last'],
                                                  rad))

        d.meta['rfb_npi'] = partial_key(rad2npi, rad) or ''

        if d.meta.get('current_smoker'):
            d.meta['current_smoker'] = 'Yes'

    csv_out = os.path.splitext(csv_file)[0]+"+ext.csv"
    DixelTools.save_csv(csv_out, worklist, fieldnames)


