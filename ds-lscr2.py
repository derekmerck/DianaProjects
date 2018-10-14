import os, logging, yaml
from diana.apis import MetaCache, Montage, LungScreeningReport

## xxxx3040 is a 4B

with open("/Users/derek/dev/DIANA/_secrets/lifespan_services.yml", 'r') as f:
    services = yaml.load(f)

logging.basicConfig(level=logging.DEBUG)

data_dir = "/Users/derek/Desktop/"
data_fn = "lscr2-rih.csv"
fp = os.path.join( data_dir, data_fn )


M = Montage(**services['montage'])

worklist = MetaCache(key_field='Exam Unique ID')
worklist.load(fp)

for item in worklist:
    logging.debug(item)

    qdict = {"q": item.meta['Exam Unique ID'],
             "modality": 4,
             "exam_type": [8274, 8903],  # 8119, 1997 (short term f/u)
             "start_date": "2018-01-01",
             "end_date": "2018-12-31"}

    results = M.find(qdict)

    if results:

        study = results.pop()

        logging.debug( repr( study.report ))
        current_smoker = LungScreeningReport.current_smoker( study.report )
        logging.debug("Current Smoker: {}".format( current_smoker ))
        pack_years = LungScreeningReport.pack_years( study.report )
        logging.debug("Pack Years: {}".format( pack_years ))
        years_since_quit = LungScreeningReport.years_since_quit( study.report )
        logging.debug("Years Quit: {}".format( years_since_quit ))

        lungrads = LungScreeningReport.lungrads(study.report)
        logging.debug("lung-rads: {}".format(lungrads))
        if lungrads:
            lungrads_val = lungrads[0]
            lungrads_s = lungrads.upper().find('S') > 0
            lungrads_c = lungrads.upper().find('C') > 0
            logging.debug("lung-rads val: {}".format(lungrads_val))
            logging.debug("lung-rads s: {}".format(lungrads_s))
            logging.debug("lung-rads c: {}".format(lungrads_c))

            signs_or_symptoms = int(lungrads_val) > 3
            logging.debug("signs_symptoms: {}".format(signs_or_symptoms))
        else:
            lungrads_val = None
            lungrads_c = None
            lungrads_s = None
            signs_or_symptoms = False

        is_annual = LungScreeningReport.is_annual(study.report)
        logging.debug("is_annual: {}".format(is_annual))

        item.meta['Smoking Status'] = 1 if current_smoker else 2
        item.meta['Number Of Packs Year Smoking'] = pack_years or 999
        item.meta['Number Of Years Since Quit'] = years_since_quit or 999 if not current_smoker else None
        item.meta['Signs Or Symptoms Of Lung Cancer'] = "Y" if signs_or_symptoms else "N"
        item.meta['Indication Of Exam'] = 2 if is_annual else 1
        item.meta['CT Exam Result Lung RADS'] = lungrads_val if lungrads_val != "4" else "4A"
        item.meta['CT Exam Result Modifier S'] = "Y" if lungrads_s else "N"
        item.meta['CT Exam Result Modifier C'] = "Y" if lungrads_c else "N"
        item.meta['ExamType'] = study.meta['ExamType']

worklist.dump(fp=os.path.join( data_dir, "lscr-dump.csv" ))
