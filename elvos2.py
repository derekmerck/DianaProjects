"""
ELVO Study Cohort
Merck, Winter 2018

Status: In progress, pending image inventory update

600+ subjects with ELVO studies and known stroke status, 2015?-2017

Workflow to scrape positives, age match normals, collect, anonymize, and download
"""

import logging
import yaml
import os
from pprint import pformat
from hashlib import md5

from DixelKit.Dixel import DicomLevel
from DixelKit import DixelTools
from DixelKit.Montage import Montage
from DixelKit.Orthanc import Orthanc, OrthancProxy
from GUIDMint import PseudoMint
from DianaConnect.check_index import check_index

LOOKUP_POS_ANS     = False
MATCH_NORMALS      = False
MERGE_WORKLIST     = False
LOOKUP_SERUIDS     = False
CLEAN_WORKLIST     = False
PULL_FROM_PACS     = True

data_root = "/Users/derek/Desktop/ELVO"
storage_root = "/Volumes/3dlab/ELVO"


def lookup_accessions(report_db, csv_fn):
    positives, fieldnames0 = DixelTools.load_csv(csv_fn,
                                                 secondary_id='ReferenceTime')
    logging.debug(positives)
    positives = report_db.update_worklist(positives, time_delta="-1d")

    csv_out = os.path.splitext(csv_fn)[0] + "+mon.csv"
    logging.debug(csv_out)
    DixelTools.save_csv(csv_out, positives)


def match_normals(data_root, pos_csv_fn, neg_csv_fn):

    pos_csv_file = os.path.join(data_root, pos_csv_fn)
    positives, = DixelTools.load_csv(pos_csv_file)
    logging.debug(positives)

    neg_csv_file = os.path.join(data_root, neg_csv_fn)
    candidates, fieldnames = DixelTools.load_csv(neg_csv_file)
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

    logging.debug(normals)
    logging.debug("After pos matching: {} (compare {})".format(len(normals),
                                                               len(positives)))

    csv_out = os.path.splitext(neg_csv_file)[0] + "+matched.csv"
    DixelTools.save_csv(csv_out, normals, fieldnames)


def merge_worklists(data_root, pos_csv_fn, neg_csv_fn):

    pos_csv_file = os.path.join(data_root, pos_csv_fn)
    positives, fieldnames0 = DixelTools.load_csv(pos_csv_file)
    logging.debug(positives)

    neg_csv_file = os.path.join(data_root, neg_csv_fn)
    normals, fieldnames1 = DixelTools.load_csv(neg_csv_file)
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

    out_csv_file = os.path.join(data_root, "worklist.csv")
    DixelTools.save_csv(out_csv_file, worklist, fieldnames0)


def lookup_seruids(csv_fn):

    csv_file = os.path.join(data_root, csv_fn)
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
    DixelTools.save_csv(csv_out, worklist, fieldnames)


def cleanup_worklist(data_root, csv_fn):
    csv_file = os.path.join(data_root, csv_fn)
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

    csv_file = os.path.join(data_root, "worklist+clean.csv")
    DixelTools.save_csv(csv_file, worklist, fieldnames)


mint = PseudoMint()
def get_anon_id(d):
    # logging.debug(pformat(d.meta))

    name = (d.meta["PatientName"]).upper()
    if d.meta.get('PatientSex'):
        gender = d.meta['PatientSex']
    elif d.meta.get("Gender"):
        gender = d.meta['Gender'][0].upper()

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

    else:
        logging.warn("Not enough meta-data for {}, need to lookup".format(d))
        return d

    logging.debug(pformat(new_id))

    d.meta['AnonID'] = new_id[0]
    d.meta['AnonName'] = new_id[1]
    d.meta['AnonDoB'] = new_id[2].replace('-', '')
    d.meta['AnonAccessionNumber'] = md5(d.meta['AccessionNumber']).hexdigest()
    return d


def anonymization_fn(d):
    if not (d.meta.get('AnonName') or d.meta.get('AnonId') or \
            d.meta.get('AnonDoB') or d.meta.get('AnonAccessionNumber')):
        logging.warn("Passed a dixel w no anonymization key, creating new one")
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


def crossref_approvals(data_root, csv_fn, ready_fns):

    csv_file = os.path.join(data_root, csv_fn)
    worklist, fieldnames = DixelTools.load_csv(csv_file)

    logging.debug("Found {} candidate positives".format(len(worklist)))

    if ready_fns:

        readylist = set()

        for fn, status in ready_fns:

            readylist_ = set()

            ready_fp = os.path.join(data_root, fn)
            with open(ready_fp, 'r') as f:
                ready_accessions = f.readlines()

            # for i, r in enumerate(ready_accessions):
            #     # r.rstrip()
            #     logging.debug((i,r))

            logging.debug("Found {} approvals".format(len(ready_accessions)))

            logging.debug("Cross-referencing")

            for AccessionNumber in ready_accessions:
                AccessionNumber = AccessionNumber.rstrip()
                # logging.debug("Looking for '{}'".format(AccessionNumber))
                for d in worklist:
                    # logging.debug("Testing '{}'".format(d.meta['AccessionNumber']))
                    if d.meta['AccessionNumber'] == AccessionNumber:
                        try:
                            assert(d.meta["ELVO on CTA?"].lower()==status.lower())
                            readylist_.add(d)
                            worklist.remove(d)
                            # logging.debug("Found pos {}".format(d))
                        except:
                            logging.error("Found a bad status match! {}!={}".format(d.meta["ELVO on CTA?"].lower(), status.lower))
                        break

            readylist = readylist.union(readylist_)
            logging.debug("Found {} cross-matches".format(len(readylist_)))

    logging.debug("Found {} total cross-matches".format(len(readylist)))
    csv_file = os.path.join(data_root, "ready.csv")
    DixelTools.save_csv(csv_file, readylist, fieldnames)



    # Need similar for worklist_cirr and worklist_hounsfield


if __name__ == "__main__":

    logging.basicConfig(level=logging.DEBUG)
    # Setup Gateways
    # ------------------------------------
    with open("secrets.yml", 'r') as f:
        secrets = yaml.load(f)
    deathstar = OrthancProxy(**secrets['services']['deathstar'])
    cirr1 = Orthanc(**secrets['services']['cirr1'])
    hounsfield = Orthanc(**secrets['services']['hounsfield+elvo'])
    montage = Montage(**secrets['services']['montage'])


    # 1. Look in Montage for report info/Accession Numbers for positives
    # ------------------------------------
    if LOOKUP_POS_ANS:
        # MRN's + date of service, need to lu accessions
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
        merge_worklists(data_root, pos_csv_fn, neg_csv_file)


    # 4. Find items on the CIRR and mark RetreiveFrom as peer
    # ------------------------------------
    # CIRR down!


    # 5. Find items on the CIRR or PACS proxy and mark RetreiveFrom as modality
    # ------------------------------------
    if LOOKUP_SERUIDS:

        csv_fn = "worklist.csv"
        lookup_seruids(data_root, csv_fn)


    # 6. Normalize some fields
    # ------------------------------------
    if CLEAN_WORKLIST:

        csv_fn = "worklist+seruids.csv"
        cleanup_worklist(data_root, csv_fn)


    # 7. Pull, anonymize, save, delete
    # ------------------------------------
    # Also, update csv along the way with anon_ids
    if PULL_FROM_PACS:

        def better_seruids(proxy, data_root, csv_fn):

            csv_file = os.path.join(data_root, csv_fn)
            worklist, fieldnames = DixelTools.load_csv(csv_file, dicom_level=DicomLevel.SERIES)

            out_file = os.path.join(data_root, "ready_rebuild.csv")

            for d in worklist:

                def approved(series_desc):
                    desc = series_desc.lower()

                    # blast!
                    return desc.find("sagital") < 0
                    # return (desc.find("thin") >= 0 or \
                    #        desc.find("1mm") >= 0) and \
                    #        not ( desc.find("sagittal") >= 0 or \
                    #              desc.find("coronal") >= 0 )

                # Known thin?
                if approved(d.meta.get("SeriesDescription")):
                    continue

                # Otherwise need to confirm
                q = {"StudyInstanceUID": d.meta['StudyInstanceUID']}
                r = proxy.query(q, level=DicomLevel.SERIES)

                if not r:
                    logging.warn("Still bad data")
                    d.meta['rebuild'] = "Review"
                    continue

                # logging.debug(pformat(r))

                def allowed(series_desc):
                    desc = series_desc.lower()
                    return desc.find('nc') < 0 and \
                        desc.find('sagittal') < 0 and \
                        desc.find('sagital') < 0 and \
                        desc.find('thick') < 0 and \
                        desc.find('mip') < 0 and \
                        desc.find('obl') < 0

                current_best = None
                for series in r:
                    if allowed(series['SeriesDescription']):
                        current_best = series
                        break

                for series in r:
                    if allowed(series['SeriesDescription']) and \
                        int(int(series['NumberOfSeriesRelatedInstances'])) > int(current_best['NumberOfSeriesRelatedInstances']):
                        # logging.warn('{} ({}) is better than {} for {} ({})'.format(
                        #     series['SeriesDescription'],
                        #     series['NumberOfSeriesRelatedInstances'],
                        #     current_best['SeriesDescription'],
                        #     current_best['NumberOfSeriesRelatedInstances'],
                        #     current_best['AccessionNumber']))
                        current_best = series
                    # else:
                    #     logging.warn('{} ({}) is NOT better than {} ({}) for {}'.format(
                    #         series['SeriesDescription'],
                    #         series['NumberOfSeriesRelatedInstances'],
                    #         current_best['SeriesDescription'],
                    #         current_best['NumberOfSeriesRelatedInstances'],
                    #         current_best['AccessionNumber']))

                if d.meta['SeriesInstanceUID'] != current_best['SeriesInstanceUID']:
                    logging.warn('{} ({}) is better than {} for {}'.format(
                        current_best['SeriesDescription'],
                        current_best['NumberOfSeriesRelatedInstances'],
                        d.meta['SeriesDescription'],
                        d.meta['AccessionNumber']))
                    logging.warn("Flagging rebuild!")
                    d.meta['SeriesInstanceUID'] = current_best['SeriesInstanceUID']
                    d.meta['SeriesDescription'] = current_best['SeriesDescription']
                    d.meta['rebuild'] = True

                    DixelTools.save_csv(out_file, worklist, fieldnames)

                else:
                    logging.debug("{} ({}) is best series".format(current_best['SeriesDescription'],
                        current_best['NumberOfSeriesRelatedInstances']))
                    d.meta['rebuild'] = False


        # better_seruids(deathstar, data_root, "ready_rebuild.csv")






        # Check existing lists for approvals

        # csv_fn = "worklist+clean.csv"
        # ready_fns = [("ready_pos.txt", "Yes"), ("ready_neg.txt", "No")]
        # crossref_approvals(data_root, csv_fn, ready_fns=ready_fns)

        # Merge w alternate normals


        def lookup_missing_dobs(index, data_root, csv_fn):

            csv_file = os.path.join(data_root, csv_fn)
            worklist, fieldnames = DixelTools.load_csv(csv_file, dicom_level=DicomLevel.SERIES)

            for d in worklist:

                if not d.meta.get('PatientBirthDate'):
                    q = """search index=dicom_series AccessionNumber="{}" | dedup AccessionNumber | fields AccessionNumber PatientID PatientBirthDate | fields - _*""".format(d.meta.get('AccessionNumber'))
                    r = check_index(index, q)
                    if r:
                        d.meta.update(r[0])
                    if d.meta.get("PatientBirthDate") is None:
                        logging.warn("Still missing birth date!")

            csv_file = os.path.join(data_root, "ready_w_anon_plus.csv")
            DixelTools.save_csv(csv_file, worklist, fieldnames)


        # from Splunk import Splunk
        # splunk = Splunk(**secrets['services']['splunk'])
        # csv_fn = "ready_w_anon.csv"
        # lookup_missing_dobs(splunk, data_root, csv_fn)



        def get_anon_ids(orthanc, data_root, csv_fn, anon_fn):

            csv_file = os.path.join(data_root, csv_fn)
            ready, fieldnames = DixelTools.load_csv(csv_file, dicom_level=DicomLevel.SERIES)

            logging.debug("Ready is {} items long".format(len(ready)))

            csv_file = os.path.join(data_root, anon_fn)
            anonymized, fieldnames = DixelTools.load_csv(csv_file, dicom_level=DicomLevel.SERIES)

            logging.debug("Anonymized is {} items long".format(len(anonymized)))
            worklist = anonymized.union(ready)

            logging.debug("Worklist is {} items long".format(len(worklist)))

            n_anonymized = 0
            for d in worklist:
                if d.meta.get('AnonID'):
                    n_anonymized = n_anonymized+1

            logging.debug("Worklist contains {} anonymized dixels".format(n_anonymized))

            for d in worklist:
                if d.meta.get("AnonID"):
                    ready.remove(d)
                    anonymized.add(d)
                    DixelTools.save_csv(csv_file, anonymized, fieldnames)
                    continue

                if not d.meta.get('PatientName'):
                    d = orthanc.update(d)

                if not d.meta.get('PatientName'):
                    logging.warn('Insufficient metadata for anonymization!')
                    continue

                d = get_anon_id(d)
                ready.remove(d)
                anonymized.add(d)
                DixelTools.save_csv(csv_file, anonymized, fieldnames)

            logging.debug("Anonymized {} dixel series.".format(len(anonymized)))
            logging.debug("Missing {} dixel series.".format(len(ready)))

        # get_anon_ids(hounsfield, data_root, "ready_w_anon.csv")
        # get_anon_ids(deathstar, data_root, "ready_w_anon_plus.csv", "ready_w_anon_plus.csv")

        def anonymize_and_save(data_root, csv_fn, save_dir, keep_anon=True, force_rebuild=False):

            csv_file = os.path.join(data_root, csv_fn)
            worklist, fieldnames = DixelTools.load_csv(
                csv_file, dicom_level=DicomLevel.SERIES)

            for d in worklist:

                if not d.meta.get("AnonID") or \
                        not d.meta.get("AnonDoB") or \
                        not d.meta.get("AnonAccessionNumber") or \
                        not d.meta.get("AnonName"):
                    logging.warn("Incomplete anonymization for {}".format(d))
                    continue

                fp = os.path.join(save_dir, d.meta['AnonID'] + '.zip')
                if os.path.exists(fp) and not (force_rebuild and d.meta.get('rebuild')):
                    logging.debug('{} already exists -- skipping'.format(d.meta['AnonID'] + '.zip'))
                    continue

                logging.debug('{} doesn\'t exist yet -- working'.format(d.meta['AnonID'] + '.zip'))

                if d.meta.get('RetrieveAETitle')=="HOUNSFIELD":
                    orthanc = hounsfield
                    if d not in hounsfield.series:
                        logging.warn("Missing patient on Hounsfield {}".format(d))
                        continue

                elif d.meta.get('RetrieveAETitle')=="GEPACS":
                    orthanc = deathstar
                    orthanc.get(d, retrieve=True, lazy=True)
                    if not d.meta.get('OID'):
                        logging.warn("Can't figure out OID for {}, apparently not retrieved".format(d))
                        continue

                else:
                    logging.warn("No AET to parse for {}, skipping".format(d))
                    continue

                e = orthanc.anonymize(d, anonymization_fn(d))
                e = orthanc.update(e)
                Orthanc.get(orthanc, e)
                e.save_archive(save_dir)

                if not keep_anon:
                    orthanc.delete(e)

        # anonymize_and_save(data_root, "ready_rebuild.csv",
        #                    os.path.join(storage_root, "anon"),
        #                    keep_anon=False,
        #                    force_rebuild=True)


        def make_meta(data_root, csv_fn, key_fn, meta_fn):

            import csv
            csv_file = os.path.join( data_root, csv_fn )
            key_file = os.path.join( data_root, key_fn )
            meta_file = os.path.join( data_root, meta_fn )

            with open(csv_file, "r") as f:
                items = csv.DictReader(f)

                items_ = []
                for item in items:
                    if item.get("AnonID"):
                        items_.append(item)

                key_fields = [
                    "AccessionNumber",
                    "PatientID",
                    "PatientName",
                    "PatientBirthDate"
                    "RetrieveAETitle",
                    "StudyInstanceUID",
                    "StudyDescription",
                    "SeriesInstanceUID",
                    "SeriesDescription",
                    "PatientBirthDate",
                    "AnonAccessionNumber",
                    "AnonID",
                    "AnonName",
                    "AnonDoB",
                    "ELVO on CTA?",
                    "Gender"
                ]

                with open(key_file, "w") as o:
                    writer = csv.DictWriter(o, key_fields, extrasaction="ignore")
                    writer.writeheader()
                    writer.writerows(items_)

                meta_map = {
                    "AnonAccessionNumber": "AccessionNumber",
                    "AnonID": "PatientID",
                    "AnonName": "PatientName",
                    "AnonDoB": "PatientBirthDate",
                    "ELVO on CTA?": "ELVO status",
                    "Gender": "PatientSex",
                }

                meta_fields = [
                    "AccessionNumber",
                    "PatientID",
                    "PatientName",
                    "PatientBirthDate",
                    "ELVO status",
                    "PatientSex",
                    "SeriesDescription"
                ]

                for item in items_:
                    for k,v in meta_map.iteritems():
                        item[v] = item[k]

                with open(meta_file, "w") as o:
                    writer = csv.DictWriter(o, meta_fields, extrasaction="ignore")
                    writer.writeheader()
                    writer.writerows(items_)


        make_meta(data_root, "ready_rebuild.csv", "elvos_key_drop1.csv", "elvos_meta_drop1.csv")


    # Create readylists...
    #
    # csv_file = os.path.join(data_root, "worklist+clean.csv")
    # worklist, fieldnames = DixelTools.load_csv(csv_file, dicom_level=DicomLevel.SERIES)
    #
    # ready_accessions_file = os.path.join(data_root, "ready_positives.txt")
    # with open(ready_accessions_file, 'r') as f:
    #     ready_accessions = f.readlines()
    #
    # readylist = set()
    #
    # for AccessionNumber in ready_accessions:
    #     AccessionNumber = AccessionNumber.rstrip()
    #     logging.debug("Looking for '{}'".format(AccessionNumber))
    #     for d in worklist:
    #         # logging.debug("Testing '{}'".format(d.meta['AccessionNumber']))
    #         if d.meta['AccessionNumber'] == AccessionNumber:
    #             assert(d.meta["ELVO on CTA?"]=="Yes")
    #             readylist.add(d)
    #             worklist.remove(d)
    #             logging.debug("Found pos {}".format(d))
    #             break
    #
    # matchlist = set()
    #
    # for d in readylist:
    #     for e in worklist:
    #         if d.meta['Age']==e.meta['Age'] and \
    #                 d.meta['Gender']==e.meta['Gender'] and \
    #                 e.meta["ELVO on CTA?"]=="No":
    #             matchlist.add(e)
    #             worklist.remove(e)
    #             logging.debug("Found neg match {} for {}".format(e, d))
    #             break
    #
    # logging.debug(len(readylist))
    # logging.debug(len(matchlist))
    #
    # # Now we want to do something like lookup all the OIDs for the readylist and figure out
    # # their AnonIDs and write them out to spreadsheet
    #
    # anon_list = set()
    # for d in readylist:
    #     if not d.meta.get('OID'):
    #         continue
    #     d = hounsfield.update(d)  # Need to get DICOM fields
    #     d = get_anon_id(d)
    #     anon_list.add(d)
    #
    # # outset = readylist.union(matchlist)
    # csv_file = os.path.join(data_root, "ready.csv")
    # DixelTools.save_csv(csv_file, anon_list, fieldnames)

