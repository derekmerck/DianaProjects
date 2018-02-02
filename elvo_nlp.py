from pprint import pprint
import nltk
from nltk.corpus import stopwords

corpus_root = '/Users/derek/Desktop/elvo_corpus'
reports = nltk.corpus.PlaintextCorpusReader(corpus_root, '.*')
# all = nltk.Text(reports.words())

toks = [w.lower() for w in reports.words() if w.isalpha() and w not in stopwords.words('english')]

all = nltk.Text(toks)
print all.concordance('hemodynamically')

#Create your bigrams
# bgs = nltk.bigrams(toks)

# tgs = nltk.ngrams(toks, 3)
#
# fdist = nltk.FreqDist(tgs)
# pprint(fdist.most_common(20))