#!/usr/bin/python

'''
Copyright (C) 2010  Cagatay Calli <ccalli@gmail.com>

Reads cat_hier file and produces extended stop category list.
Input format: <cat id> <list of immediate descendants cat ids>

USAGE: scanCatHier.py <cat_hier output file path>

IMPORTANT: If you use XML output from a recent version of Wikiprep
(e.g. Zemanta fork), then set FORMAT to 'Zemanta-legacy' or 'Zemanta-modern'.

'''

import sys
import re

FORMAT = 'Gabrilovich'


# read list of stop categories from 'wiki_stop_categories.txt'
STOP_CATS = []
try:   
        f = open('wiki_stop_categories.txt','r')
        for line in f.readlines():
                [strId,strCat] = line.split('\t')
                STOP_CATS.append(int(strId))
        f.close()
except:
        print 'Stop categories cannot be read! Please put "wiki_stop_categories.txt" file containing stop categories in this folder.'
        sys.exit(1)

catDict = {}

args = sys.argv[1:]
if not args:
	sys.exit(1)

f = open(args[0],'r')	# cat_hier file
for i in range(3):
	f.readline()

for line in f.readlines():
	parts = line.split('\t',1)
	if len(parts) == 2:
		parent = int(parts[0])
		cs = parts[1].split()
		childs = []
		for c in cs:
			if c:
				c = int(c)
				childs.append(c)

		catDict[parent] = childs
	
f.close()

print 'cat_hier output complete'
print 'traversing category tree..'

cats = set(STOP_CATS)
outcats = set(STOP_CATS)

#allCatSet = frozenset(catList)

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
f = open('ecat.txt','w')
for c in outcats:
	f.write(str(c) + '\n')
f.close()
