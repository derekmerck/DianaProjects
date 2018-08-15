import os
from diana.apis import MetaCache

data_dir = "/Users/derek/Projects/Medical Informatics/NLP RADCAT/"
audited_fn = "reconcile_meta+audit.jm-complete.csv"
fp = os.path.join( data_dir, audited_fn )

worklist = MetaCache(key_field='id')
worklist.load(fp)

for item in worklist:
    if item.meta['radcat'] == "3":
        item.meta['radcat'] = "2"
        item.meta['radcat3'] = "Yes"

worklist.dump(fp= os.path.join( data_dir, "radcat700.jm18.csv" ))

matches = 0
for item in worklist:

    if item.meta['radcat'] < "3" and \
        item.meta['audit_radcat'] < "3": # and \
        # item.meta['radcat3'] == item.meta['audit_radcat3']:
        matches += 1

    elif item.meta['radcat'] == item.meta['audit_radcat']: # and \
        # item.meta['radcat3'] == item.meta['audit_radcat3']:
        matches += 1

print("Matches: {}/{} = {:.2}".format(matches, len(worklist), matches/len(worklist)))