import logging
import yaml
import os
from DixelKit import DixelTools
from DixelKit.Montage import Montage
from DixelKit.Splunk import Splunk

if __name__ == "__main__":

    logging.basicConfig(level=logging.DEBUG)
    with open("secrets.yml", 'r') as f:
        secrets = yaml.load(f)

    # 1. Create worklist from all studies with "RADCAT\d"

    qdict = { "q":          "RADCAT",
              "start_date": "2017-11-01",
              "end_date":   "2018-01-29"}

    worklist = Montage.make_worklist(qdict)
    logging.debug(worklist)

    DixelTools.report_extractions()


    # Default category values for this data
    for d in worklist:
        d.meta['categories'] = ['cta', 'head', 5]

    # DixelTools.save_text_corpus('/Users/derek/Desktop/elvo_corpus', worklist)



    # 1. Look in Montage for report info/Accession Numbers
    # ------------------------------------

    # montage = Montage(**secrets['services']['montage'])
    # worklist = montage.update_worklist(worklist, time_delta="-1d")
    #
    # csv_out = os.path.splitext(csv_file)[0]+"+mon.csv"
    # logging.debug(csv_out)
    # DixelTools.save_csv(csv_out, worklist)


    # 2. Find items in the CIRR w Splunk and mark RetreiveFrom as CIRR
    # ------------------------------------


    # 3. Find items on the PACS proxy and mark RetreiveFrom as modality
    # ------------------------------------

    deathstar = OrthancProxy(**secrets['services']['deathstar'])

    # qdict = {'SeriesDescription': '*axial*brain*cta*'}
    # worklist = deathstar.update_worklist(worklist, qdict=qdict)
    #
    # qdict = {'SeriesDescription': '*brain*cta*'}
    # worklist = deathstar.update_worklist(worklist, qdict=qdict)
    #
    # qdict = {'SeriesDescription': '*arterial*thin*axial'}
    # worklist = deathstar.update_worklist(worklist, qdict=qdict)
    #
    # qdict = {'SeriesDescription': 'CTA Head and Neck'}
    # worklist = deathstar.update_worklist(worklist, qdict=qdict)
    #
    # qdict = {'SeriesDescription': 'arterial axial reformat'}
    # worklist = deathstar.update_worklist(worklist, qdict=qdict)
    #
    # csv_out = os.path.splitext(csv_file)[0]+"+out.csv"
    # logging.debug(csv_out)
    # DixelTools.save_csv(csv_out, worklist)


    # 4. Copy data from sources
    # ------------------------------------

    hounsfield = Orthanc(**secrets['services']['hounsfield+elvo'])

    # cirr1.copy_worklist(worklist, hounsfield)

    worklist1 = set()
    for d in worklist:
        if d.meta['RetrieveAETitle'] == "GEPACS":
            worklist1.add(d)

    logging.debug(worklist1)
    deathstar.get_worklist(worklist1, retrieve=True)

