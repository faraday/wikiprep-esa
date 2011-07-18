#!/usr/bin/python
# -*- coding: utf-8 -*-

'''
Copyright (C) 2010  Cagatay Calli <ccalli@gmail.com>

Scans XML output (gum.xml) from Wikiprep, creates 3 tables:

TABLE: article   	COLUMNS: id INT, title VARBINARY(255)
TABLE: text 	 	COLUMNS: old_id INT, old_text MEDIUMBLOB
TABLE: pagelinks	COLUMNS: source_id INT, target_id INT

USAGE: scanData.py <hgw.xml/gum.xml file from Wikiprep> --format=<Wikiprep dump format> [--stopcats=<stop category file>]

IMPORTANT: If you use XML output from a recent version of Wikiprep
(e.g. Zemanta fork), then set FORMAT to 'Zemanta-legacy' or 'Zemanta-modern'.


ABOUT STOP CATEGORY FILTERING:
Stop category filtering is not active in default configuration. You can change
 provide an updated list of stop categories, derived from your Wikipedia dump 
with --stopcats option.
e.g. scanData.py sample.gum.xml --stopcats=sampleCategoryList.txt 

Cleaning up irrelevant articles is important in ESA so providing such a file 
is recommended.

'''

import sys
import re
import MySQLdb
import signal
from optparse import OptionParser

import lxml.html as html
import Stemmer

import xmlwikiprep

# Wikiprep dump format enum
# formats: 1) Gabrilovich 2) Zemanta-legacy 3) Zemanta-modern
F_GABRI = 0 # gabrilovich
F_ZLEGACY = 1	# zemanta legacy
F_ZMODERN = 2	# zemanta modern

usage = """
USAGE: scanData.py <hgw.xml/gum.xml file from Wikiprep> --format=<Wikiprep dump format> [--stopcats=<stop category file>]

Wikiprep dump formats:
1. Gabrilovich [gl, gabrilovich]
2. Zemanta legacy [zl, legacy, zemanta-legacy]
3. Zemanta modern [zm, modern, zemanta-modern]

'2005_wiki_stop_categories.txt' can be used for 2005 dump of Gabrilovich et al.
"""
parser = OptionParser(usage=usage)
parser.add_option("-s", "--stopcats", dest="stopcats", help="Path to stop categories file", metavar="STOPCATS")
parser.add_option("-f", "--format", dest="_format", help="Wikiprep dump format (g for Gabrilovich, zl for Zemanta-legacy,zm for Zemanta-modern)", metavar="FORMAT")


(options, args) = parser.parse_args()
if not args:
	print usage
	sys.exit()
if not options.stopcats:
        print 'Stop category list is not provided. (You can provide this with --stopcats argument.)'
	print 'Continuing without stop category filter...'

if not options._format:
	print """
Wikiprep dump format not specified! Please select one from below with --format option:

Wikiprep dump formats:
1. Gabrilovich [gl, gabrilovich]
2. Zemanta legacy [zl, legacy, zemanta-legacy]
3. Zemanta modern [zm, modern, zemanta-modern]
"""
	sys.exit()

if options._format in ['zm','zemanta-modern','Zemanta-modern','Zemanta-Modern','modern']:
	FORMAT = F_ZMODERN
elif options._format in ['gl','gabrilovich','Gabrilovich']:
	FORMAT = F_GABRI
elif options._format in ['zl','zemanta-legacy','Zemanta-legacy','Zemanta-Legacy','legacy']:
	FORMAT = F_ZLEGACY	


# scanData.py <hgw_file> [--stopcats=<stop category file>]

hgwpath = args[0] # hgw/gum.xml

TITLE_WEIGHT = 4
STOP_CATEGORY_FILTER = bool(options.stopcats)

# reToken = re.compile('[a-zA-Z\-]+')
reToken = re.compile("[^ \t\n\r`~!@#$%^&*()_=+|\[;\]\{\},./?<>:’'\\\\\"]+")
reAlpha = re.compile("^[a-zA-Z\-_]+$")
NONSTOP_THRES = 100

STEMMER = Stemmer.Stemmer('porter')

# read stop word list from 'lewis_smart_sorted_uniq.txt'
wordList = []
try:
	f = open('lewis_smart_sorted_uniq.txt','r')
	for word in f.readlines():
		wordList.append(word.strip())
	f.close()
except:
	print 'Stop words cannot be read! Please put "lewis_smart_sorted_uniq.txt" file containing stop words in this folder.'
	sys.exit(1)

STOP_WORDS = frozenset(wordList)


if STOP_CATEGORY_FILTER:
	# read list of stop categories from 'extended_stop_categories.txt'
	catList = []
	try:
		f = open(options.stopcats,'r')
		for line in f.readlines():
			strId = line.split('\t')[0]
			if strId:
				catList.append(int(strId))
		f.close()
	except:
		print 'Stop categories cannot be read!'
		sys.exit(1)

	STOP_CATS = frozenset(catList)


# read disambig IDs for legacy format
disambigList = []
if FORMAT != F_ZMODERN:
	disambigPath = hgwpath.replace('hgw.xml','disambig')
	print disambigPath
	try:
		f = open(disambigPath, 'r')

		for i in range(3):
        		f.readline()

		prevId = ''
		for line in f.readlines():
        		if prevId and line.startswith(prevId):
	                	continue
	        	id = line.split('\t',1)[0].strip()
		        disambigList.append(int(id))
        		prevId = id

		f.close()
	except:
		print 'Disambig file cannot be read! Please check if a file with .disambig suffix exists in Wikiprep dump location'
		sys.exit(1)

DISAMBIG_IDS = frozenset(disambigList)

try:
	conn = MySQLdb.connect(host='localhost',user='root',passwd='123456',db='wiki',charset = "utf8", use_unicode = True)
except MySQLdb.Error, e:
	print "Error %d: %s" % (e.args[0], e.args[1])
	sys.exit(1)

try:
	cursor = conn.cursor()
	cursor.execute("DROP TABLE IF EXISTS article")
	cursor.execute("""
		CREATE TABLE article
		(
		  id	INT(10),
		  title	VARBINARY(255) NOT NULL,
		  PRIMARY KEY (id),
		  KEY title (title(32))
		) DEFAULT CHARSET=binary
	""")

	cursor.execute("DROP TABLE IF EXISTS text")
	cursor.execute("""
		CREATE TABLE text 
		(
		  old_id INT(10) unsigned NOT NULL,
		  old_text MEDIUMBLOB NOT NULL,
		  PRIMARY KEY (old_id)
		) DEFAULT CHARSET=binary MAX_ROWS=10000000 AVG_ROW_LENGTH=10240;
	""")

except MySQLdb.Error, e:
	print "Error %d: %s" % (e.args[0], e.args[1])
	sys.exit (1)


## handler for SIGTERM ###
def signalHandler(signum, frame):
    global conn, cursor
    cursor.close() 
    conn.close()
    sys.exit(1)

signal.signal(signal.SIGTERM, signalHandler)
#####

reOtherNamespace = re.compile("^(User|Wikipedia|File|MediaWiki|Template|Help|Category|Portal|Book|Talk|Special|Media|WP|User talk|Wikipedia talk|File talk|MediaWiki talk|Template talk|Help talk|Category talk|Portal talk):.+",re.DOTALL)

# category, disambig, stub pages are removed by flags

# regex as article filter (dates, lists, etc.)
'''
# slightly improved title filter, filtering all dates,lists etc.

re_strings = ['^(January|February|March|April|May|June|July|August|September|October|November|December) \d+$',
	      '^\d+((s|(st|nd|th) (century|millenium)))?( (AD|BC|AH|BH|AP|BP))?( in [^$]+)?$',
	      '.+\(disambiguation\)']
'''

# title filter of Gabrilovich et al. contains: * year_in ... * month_year * digit formats
re_strings = ['^(January|February|March|April|May|June|July|August|September|October|November|December) \d{4}$',
	      '^\d{4} in [^$]+?$',
	      '^\d+$']
piped_re = re.compile( "|".join( re_strings ) , re.DOTALL|re.IGNORECASE)

# list filter
reList = re.compile('^List of .+',re.DOTALL|re.IGNORECASE)


###
articleBuffer = []	# len: 100  / now: 200
aBuflen = 0

textBuffer = []		# same as articleBuffer, stores text

###

inlinkDict = {}
outlinkDict = {}

cursor.execute("SELECT i.target_id, i.inlink FROM inlinks i")
rows = cursor.fetchall()
for row in rows:
	inlinkDict[row[0]] = row[1]

cursor.execute("SELECT o.source_id, o.outlink FROM outlinks o")
rows = cursor.fetchall()
for row in rows:
	outlinkDict[row[0]] = row[1]

# for logging
# Filtered concept id=12 (hede hodo) [minIncomingLinks]
log = open('log.txt','w')

# pageContent - <page>..content..</page>
# pageDict - stores page attribute dict
def recordArticle(pageDoc):
   global articleBuffer, textBuffer, aBuflen, STEMMER

   if FORMAT == F_ZMODERN and (pageDoc['disambig'] or pageDoc['category'] or pageDoc['image']):
	return

   # a simple check for content
   if pageDoc['length'] < 10:
	return

   title = pageDoc['title']
   _id = pageDoc['_id']

   # only keep articles of Main namespace
   if reOtherNamespace.match(title):
        return

   # skip disambig   
   if FORMAT != F_ZMODERN and _id in DISAMBIG_IDS:
	return

   # ** stop category filter **
   if STOP_CATEGORY_FILTER:
   	cats = frozenset(pageDoc['categories'])

   	# filter article with no category or belonging to stop categories
   	if not cats or STOP_CATS.intersection(cats):
        	log.write('Filtered concept id='+str(_id)+' ('+ title +') [stop category]\n')
		return
   # ******

   # ** title filter **
   if piped_re.match(title):
       log.write('Filtered concept id='+str(_id)+' ('+ title +') [regex]\n')
       return

   '''if reList.match(title):
       log.write('Filtered concept id='+str(id)+' ('+ title +') [list]\n')
       return'''
   # ******

   # ** inlink-outlink filter **
   if not inlinkDict.has_key(_id) or inlinkDict[_id] < 5:
        log.write('Filtered concept id='+str(_id)+' ('+ title.encode('utf8') +') [minIncomingLinks]\n')
	return

   if not outlinkDict.has_key(_id) or outlinkDict[_id] < 5:
        log.write('Filtered concept id='+str(_id)+' ('+ title.encode('utf8') +') [minOutgoingLinks]\n')
	return
   # ******

   text = pageDoc['text']

   # convert HTML to plain text
   t = html.fromstring(title)
   ctitle = t.text_content()

   ctext = '' 
   t = html.fromstring(text)
   ctext = t.text_content()

   # filter articles with fewer than 100 -UNIQUE- non-stop words
   cmerged = ctitle + ' \n ' + ctext

   tokens = set()
   wordCount = 0
   for m in reToken.finditer(cmerged):
	w = m.group()
	if not w or len(w) <= 2 or not reAlpha.match(w):
		continue
	lword = w.lower()
	if not lword in STOP_WORDS:
		sword = STEMMER.stemWord(STEMMER.stemWord(STEMMER.stemWord(lword)))	# 3xPorter
		if not sword in tokens:
			wordCount += 1
			tokens.add(sword)
			if wordCount == NONSTOP_THRES:
				break

   if wordCount < NONSTOP_THRES:
        log.write('Filtered concept id='+str(_id)+' ('+ title.encode('utf8') +') [minNumFeaturesPerArticle]\n')
	return


   cadd = ''
   for i in range(TITLE_WEIGHT):
	cadd += ctitle + ' \n '
   cadd += ctext

   # write article info (id,title,text)
   articleBuffer.append((_id,ctitle.encode('utf8')))
   textBuffer.append((_id,cadd.encode('utf8')))
   aBuflen += 1

   if aBuflen >= 200:
	cursor.executemany("""
		INSERT INTO article (id,title)
		VALUES (%s,%s)
		""",articleBuffer)
	cursor.executemany("""
		INSERT INTO text (old_id,old_text)
		VALUES (%s,%s)
		""",textBuffer)
	articleBuffer = []
	textBuffer = []
	aBuflen = 0

   return


f = open(hgwpath,'r')
for doc in xmlwikiprep.read(f):
	recordArticle(doc)
f.close()

if aBuflen > 0:
	cursor.executemany("""
		INSERT INTO article (id,title)
		VALUES (%s,%s)
		""",articleBuffer)
	cursor.executemany("""
		INSERT INTO text (old_id,old_text)
		VALUES (%s,%s)
		""",textBuffer)
	articleBuffer = []
	textBuffer = []

#cursor.execute("DROP TABLE outlinks")

# remove links to articles that are filtered out
cursor.execute("DROP TABLE IF EXISTS tmppagelinks")
cursor.execute("CREATE TABLE tmppagelinks LIKE pagelinks")
cursor.execute("INSERT tmppagelinks SELECT * FROM pagelinks WHERE EXISTS (SELECT id FROM article WHERE id = target_id) AND EXISTS (SELECT id FROM article WHERE id = source_id)")
cursor.execute("DROP TABLE pagelinks")
cursor.execute("RENAME TABLE tmppagelinks TO pagelinks")

cursor.execute("SELECT COUNT(id) FROM article")
r = cursor.fetchone()
print "Articles: ", r[0]

# release DB resources
cursor.close()
conn.close()

log.close()

