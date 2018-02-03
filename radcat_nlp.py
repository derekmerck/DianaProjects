import logging
import yaml
import os
from DixelKit import DixelTools
from DixelKit.DixelTools import Caching
import glob
from itertools import chain
import nltk
from nltk.corpus import stopwords
from nltk.probability import FreqDist
from nltk.classify import NaiveBayesClassifier as nbc
from nltk.corpus import CategorizedPlaintextCorpusReader


def create_corpus_from_query(montage):
    pass

    # 1. Create worklist from all studies with "RADCAT\d"
    #    Need to do this every 2 weeks or so, or you run into the
    #    25k study limit!

    # qdict = { "q":          "RADCAT",
    #           "start_date": "2017-11-01",
    #           "end_date":   "2018-01-29"}
    #
    # worklist = Montage.make_worklist(qdict)

def create_corpus_from_csv(source_dir, output_dir):
    files = glob.glob('{}/*.csv'.format(source_dir))

    logging.debug(files)

    worklist = set()

    for fn in files:
        w, fieldnames = DixelTools.load_csv(fn)
        worklist = worklist.union(w)

    logging.debug("Found {} dixels".format(len(worklist)))

    # Default category values for this data
    for d in worklist:
        d = DixelTools.report_extractions(d)
        if not d.meta.get('radcat'):
            logging.warn("Couldn't find radcat in {}".format(d.meta['AccessionNumber']))
            logging.warn(d.meta['Report Text'])
            # raise Exception("Couldn't find radcat in {}".format(d.meta['AccessionNumber']))
        else:
            logging.info("Found radcat {} in {}".format(d.meta['radcat'], d.meta['AccessionNumber']))
            d.meta['categories'] = [d.meta['radcat']]

    DixelTools.save_text_corpus(output_dir, worklist, num_subdirs=1)

def examine_corpus(corpus_dir):

    def init_documents(f_re, cat_re):
        logging.debug("Reading corpus")
        reports = CategorizedPlaintextCorpusReader(corpus_dir,
                                                   f_re,
                                                   cat_pattern=cat_re,
                                                   encoding='utf8')
        logging.debug("Found {} fileids".format(len(reports.fileids())))
        logging.debug("Found categories: {}".format(reports.categories()))
        logging.debug("Building docs")
        stop = stopwords.words('english')
        documents = [
            ([w.lower() for w in reports.words(i) if
              w.lower() not in stop and
              w.isalpha()],
             reports.categories(i)[0])
            for i in reports.fileids()]
        return documents

    documents = Caching(init_documents, 'docs', r'.*\.txt', r'.*\+(\d)\.txt')

    def init_tokens():
        tokens = list(chain(*[i for i, j in documents.data]))
        return tokens

    all_tokens = Caching(init_tokens, 'toks')

    def init_wordfeats():
        logging.debug('Creating word features')
        word_features = FreqDist(all_tokens.data)
        word_features = word_features.keys()[:100]
        return word_features

    word_features = Caching(init_wordfeats, 'wfs')

    def init_classifier(tag_func):

        logging.debug("Creating classifier")
        numtrain = int(len(documents.data) * 90 / 100)

        train_set = [({i: (i in tokens) for i in word_features.data}, tag_func(tag))
                     for tokens, tag in documents.data[:numtrain]]
        test_set  = [({i: (i in tokens) for i in word_features.data}, tag_func(tag))
                     for tokens, tag in documents.data[numtrain:]]

        logging.debug('Starting training')
        classifier = nbc.train(train_set)
        logging.debug('Trained')

        return classifier, test_set

    def eq_one(tag):
        return int(tag)==1
    classifier_eq_one = Caching(init_classifier, 'cls1', eq_one)

    print "Overall Accuracy Eq One: {}".format(nltk.classify.accuracy(
        classifier_eq_one.data[0], classifier_eq_one.data[1]))
    classifier_eq_one.data[0].show_most_informative_features(10)

    def eq_five(tag):
        return int(tag)==5
    classifier_eq_five = Caching(init_classifier, 'cls5', eq_five)

    print "Overall Accuracy Eq Five: {}".format(nltk.classify.accuracy(
        classifier_eq_five.data[0], classifier_eq_five.data[1]))
    classifier_eq_five.data[0].show_most_informative_features(10)

    text = nltk.Text(all_tokens.data)
    logging.debug("Building concordances")
    print text.concordance('anterolisthesis')
    print text.concordance('lithiasis')


if __name__ == "__main__":

    logging.basicConfig(level=logging.DEBUG)
    with open("secrets.yml", 'r') as f:
        secrets = yaml.load(f)

    # create_corpus_from_csv(
    #     '/Users/derek/Desktop/RADCAT Source',
    #     '/Users/derek/Desktop/radcat_corpus')

    examine_corpus('/Users/derek/Desktop/radcat_corpus')


