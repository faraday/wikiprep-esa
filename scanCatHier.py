#!/usr/bin/python

'''
Copyright (C) 2010  Cagatay Calli <ccalli@gmail.com>

Scans XML output (gum.xml) from Wikiprep, category list (given with --stopcats argument)
 and outputs an extended category list, containing immediate descendants of given categories.
Output format: <category id> <category title>

USAGE: scanCatHier.py <hgw.xml file from Wikiprep> <output file path>

'''

import sys
import re
from optparse import OptionParser
import xmlwikiprep

usage = 'Usage: scanCatHier.py <hgw.xml/gum.xml file from Wikiprep> <output file path> --stopcats=<stop category file>'
parser = OptionParser(usage=usage)
parser.add_option("-s", "--stopcats", dest="stopcats", help="Path to stop categories file", metavar="STOPCATS")

(options, args) = parser.parse_args()
if not (args and len(args) == 2 and options.stopcats):
        # print USAGE
        print 'USAGE: scanCatHier.py <hgw.xml/gum.xml file from Wikiprep> <output file path> --stopcats=<stop category file>'
        sys.exit()


# read list of stop categories from 'wiki_stop_categories.txt'
STOP_CATS = []
try:   
        f = open(options.stopcats,'r')
        for line in f.readlines():
                ps = line.split('\t')
		strId = ps[0]
                STOP_CATS.append(int(strId))
        f.close()
except:
        print 'Category list cannot be read!'
        sys.exit(1)


reCategory = re.compile("^Category:.+",re.DOTALL)

catDict = {}
catTitles = {}

# pageContent - <page>..content..</page>
# pageDict - stores page attribute dict
def recordArticle(pageDoc):
   global catDict,catTitles

   title = pageDoc['title']
   if not reCategory.match(title):
	return

   curId = pageDoc['_id']

   catTitles[curId] = title

   cats = pageDoc['categories']

   cs = []
   for c in cats:
	if catDict.has_key(c):
		catDict[c].add(curId)
	else:
		catDict[c] = set([curId])

   return


# scanCatHier.py <hgw/gum.xml> --stopcats=<category list file>

f = open(args[0],'r')

for doc in xmlwikiprep.read(f):
	recordArticle(doc)

f.close()

print 'cat_hier output complete'
print 'traversing category tree..'

cats = set(STOP_CATS)
outcats = set(STOP_CATS)

while cats:
	parent = cats.pop()

	childs = []
	if catDict.has_key(parent):
		childs = catDict[parent]

	# avoid cycles/repeats
	for c in childs:
		if not c in outcats:
			cats.add(c)
			outcats.add(c)

# write extended stop category list
f = open(args[1],'w')
for c in outcats:
	f.write(str(c) + '\t' + catTitles[c] + '\n')
f.close()
