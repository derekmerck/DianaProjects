"""
U/S Thyroid Biopsy Cohort
Merck, Winter 2018

421 subjects with u/s biopsy and confirmed cancer status, 2016-2018
"""

from DianaFuture.dcache import CSVCache
import logging
import glob
import pytz
import dateutil.parser

"""
Load Montage and Pathology spreadsheets and correlate subjects that
exist in both datasets, i.e., patients with confirmed cancers.
"""

logging.basicConfig(level=logging.DEBUG)


def normalize_date(date_str):
    return dateutil.parser.parse(date_str)

# All Montage input
M = CSVCache("/Users/derek/Desktop/thyroid/montage_thyroid.csv",
             id_field="Accession Number")

for item in M.cache.itervalues():
    item["Exam Completed Date"] = normalize_date(item["Exam Completed Date"])

# All output

fieldnames = [
    "Patient MRN",
    "Patient First Name",
    "Patient Last Name",
    "Accession Number",
    "Exam Completed Date",
    'Pathology Case',
    'Pathology Date',
    'Cancer status'
]
Q = CSVCache("/Users/derek/Desktop/thyroid/merged.csv",
             id_field="Accession Number", fieldnames=fieldnames, clear=True)


def compare(M, P, Q, status):

    for item in P.cache.itervalues():
        item['Patient First Name'] = item['FIRST']
        item['Patient Last Name'] = item['LAST']
        item['Pathology Case'] = item['CASE']
        item['Pathology Date'] = normalize_date(item['SIGNOUT DATE'])
        item['Cancer status'] = status

    keys = ['Patient First Name', 'Patient Last Name']

    for m in M.cache.itervalues():
        mm = dict((k, m[k]) for k in keys if k in m)
        for p in P.cache.itervalues():
            pp = dict((k, p[k]) for k in keys if k in p)

            tm = m['Exam Completed Date'].replace(tzinfo=pytz.UTC)
            tp = p['Pathology Date'].replace(tzinfo=pytz.UTC)

            if (mm == pp) and abs((tp - tm).days) < 7:
                # logging.debug("Found a match: {} - {}".format(mm, pp))

                # logging.debug(tp - tm)

                m.update(p)
                # logging.debug(m)
                Q.put(m['Accession Number'], m)
                break

# Need a set of P's
path_results = glob.glob("/Users/derek/Desktop/thyroid/path*.csv")
logging.debug(path_results)

total_path = 0

for p in path_results:
    P = CSVCache(p, id_field="CASE")
    total_path = total_path + len(P.cache)
    status = "Positive" if "pos" in p else "Negative"
    compare(M, P, Q, status)

Q.save_csv()

# Montage
logging.debug("M total: {}".format(len(M.cache)))

# Pathology
logging.debug("P total: {}".format(total_path))

# Intersection
pos = [q for q in Q.cache.itervalues() if q['Cancer status']=="Positive"]
logging.debug("Q total: {}".format( len(Q.cache) ) )
logging.debug("Q pos:   {}".format( len(pos) ) )
logging.debug("Q %pos:  {}".format( ( float(len(pos))/float(len(Q.cache)) ) * 100 ) )

