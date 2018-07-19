"""
Abdominal Aortic Aneurysm Data Pull
Merck, Summer 2018

2 parts:

1. Read in 6 months of Montage reports

  - Anonymize reports, convert to labelled corpus format for AAA NLP project w Rubin (Stanford)
  - Collate study key for AAA image AI project

"""

# ---------------------------
# Keep this before any diana imports
from config import services
# ---------------------------

import os, logging, glob, datetime
from pprint import pprint, pformat
from diana.apis import MetaCache, Orthanc, DicomFile, ReportFile, RadiologyReport
from diana.daemon import Porter

logging.basicConfig(level=logging.DEBUG)

# --------------
# SCRIPT CONFIG
# --------------

data_dir = "/Users/derek/Projects/Body/AAA/data"
input_fg = "AAA*.csv"
key_fn   = "aaa_ai.key.csv"

corpus_save_dir = "/Users/derek/data/reports/AAA"
dcm_save_dir = ""

proxy_service = "proxy2"
proxy_domain = "gepacs"

INIT_CACHE = True
CREATE_CORPUS = False
PULL_FROM_PACS = False

# Setup services
dixels = MetaCache()
proxy  = Orthanc(**services[proxy_service])
dicom_files = DicomFile(location=dcm_save_dir)
report_files  = ReportFile(location=corpus_save_dir)

# Load Montage format spreadsheet, find UIDs, set sham id
if INIT_CACHE:

    keymap = MetaCache.montage_keymap.update({
        'incidental': "aaa_incidental",    # This is inconsistently noted
        'size': "aaa_size",
        'gave rec': "aaa_gave_rec",
        "followed guidelines": "aaa_followed_guidelines",
        "impression": "aaa_in_impression",  # This is inconsistently noted
    })

    # Lisa's files are all _positive_ for AAA, need negative as well.
    fg = os.path.join(data_dir, input_fg)
    for fn in glob.glob(fg):
        fp = os.path.join(data_dir, fn)
        dixels.load(fp, keymap=MetaCache.montage_keymap )
        # logging.debug( pformat(  dixels.cache ) )

    logging.debug("{:<9} : {:19} : {:<9} : {:<15} : {:<5}".format(
        "Accession", "Date", "Size", "Rec", "Guidelines") )

    for d in dixels:

        approx_dob = datetime.date(year=2018, month=1, day=1) - \
                     datetime.timedelta(weeks=54*int(d.meta['PatientAge']))
        d.meta['PatientBirthDate'] = approx_dob
        d.meta['PatientName'] = "{}".format(d.meta['PatientID'])

        # d.set_shams()


        # Remove non-incidental or knowns (inconsistently reported)
        if d.meta.get("aaa_incidental", "").lower().startswith("n") or \
                d.meta.get("aaa_incidental", "").lower().startswith("k"):
            d.meta['aaa_size'] = "known"
            d.meta['aaa_gave_rec'] = ""
            d.meta['aaa_followed_guidelines'] = ""

        # If size is present, evaluate the for recommendation and guidelines
        elif d.meta.get("aaa_size"):
            # there was an aaa noted

            if "no size" in d.meta.get("aaa_size").lower():
                d.meta['aaa_size'] = "Not noted"
                d.meta["aaa_gave_rec"] = False
                d.meta["aaa_followed_guidelines"] = False

            elif d.meta.get("aaa_gave_rec", "").lower().startswith("y"):
                d.meta["aaa_gave_rec"] = True

                # logging.debug( d.meta.get("aaa_followed_guidelines", "") )
                if d.meta.get("aaa_followed_guidelines", "").lower().startswith("y"):
                    d.meta["aaa_followed_guidelines"] = True
                elif d.meta.get("aaa_followed_guidelines", "").lower().startswith("n"):
                    d.meta["aaa_followed_guidelines"] = False

            elif d.meta.get("aaa_gave_rec", "").lower().startswith("n"):
                d.meta["aaa_gave_rec"] = False
                d.meta["aaa_followed_guidelines"] = False


        logging.debug("{:<9} : {} : {:<9} : {:<15} : {:<5}  ".format(
                            d.meta.get("AccessionNumber"),
                            d.meta.get("StudyDate"),
                            # d.meta.get("aaa_incidental", ""),
                            d.meta.get('aaa_size', ""),
                            d.meta.get('aaa_gave_rec', ""),
                            d.meta.get('aaa_followed_guidelines', "") ) )
                            # d.meta.get('aaa_in_impression', "" ) ) )

        d.report.anonymize()
        d.meta["_report"] = d.report.text

    for d in dixels:
        logging.debug( d.report )

    logging.debug( len( dixels.cache ) )

    fp = os.path.join(data_dir, key_fn)
    dixels.dump(fp=fp,
                extra_fieldnames=['aaa_size',
                                  'aaa_gave_rec',
                                  'aaa_followed_guidelines'])

    #
    # for d in dixels:
    #     # Investigate to get UIDs
    #     proxy.find_item(d, proxy_domain)
    #     # set shams after investigation so you have complete dicom-format patient name
    #     set_shams(d)
    #
    # # Everything we need to create a key file
    # fp = os.path.join(data_dir, key_fn)
    # dixels.dump(fp)


# Exfiltrate, anonymize, stash to disk
if PULL_FROM_PACS:
    # Start from the key file or other cache
    fp = os.path.join(data_dir, key_fn)
    dixels.load(fp)

    P = Porter(source=proxy, dest=dicom_files, proxy_domain=proxy_domain)
    P.run(dixels)
