#!/usr/bin/python
# -*- coding: utf-8 -*-

'''
Copyright (C) 2010  Cagatay Calli <ccalli@gmail.com>

Scans XML output (gum.xml) from Wikiprep, creates 3 tables:

TABLE: article   	COLUMNS: id INT, title VARBINARY(255)
TABLE: text 	 	COLUMNS: old_id INT, old_text MEDIUMBLOB
TABLE: pagelinks	COLUMNS: source_id INT, target_id INT

USAGE: scanData.py <hgw.xml file from Wikiprep>

IMPORTANT: If you use XML output from a recent version of Wikiprep
(e.g. Zemanta fork), then set FORMAT to 'Zemanta-legacy' or 'Zemanta-modern'.

'''

import sys
import re
import MySQLdb
import signal

import lxml.html as html
import Stemmer

# scanData.py <hgw_file> <RSIZE>
args = sys.argv[1:]

if len(args) < 1:
    sys.exit()

if len(args) == 2:
    RSIZE = int(args[1])

hgwpath = args[0]

# formats: 1) Gabrilovich 2) Zemanta-legacy 3) Zemanta-modern
FORMAT = 'Gabrilovich'

TITLE_WEIGHT = 4

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

# read list of stop categories from 'extended_stop_categories.txt'
catList = []
try:
	f = open('wiki_stop_categories.txt','r')
	for line in f.readlines():
		strId = line.split('\t')[0]
		if strId:
			catList.append(int(strId))
	f.close()
except:
	print 'Stop categories cannot be read! Please put "extended_stop_categories.txt" file containing stop categories in this folder.'
	sys.exit(1)

STOP_CATS = frozenset(catList)


# read disambig IDs for legacy format
disambigList = []
if FORMAT != 'Zemanta-modern':
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

rePageLegacy = re.compile('<page id="(?P<id>\d+)".+?newlength="(?P<len>\d+)" stub="(?P<stub>\d)".+?>(?P<page>.+?)</page>',re.MULTILINE | re.DOTALL)

rePageModern = re.compile('<page id="(?P<id>\d+)".+?newlength="(?P<len>\d+)" stub="(?P<stub>\d)" disambig="(?P<disambig>\d)" category="(?P<cat>\d)" image="(?P<img>\d)">(?P<page>.+?)</page>',re.MULTILINE | re.DOTALL)

reContent = re.compile('<title>(?P<title>.+?)</title>\n<categories>(?P<categories>.*?)</categories>.+?<text>(?P<text>.+?)</text>',re.MULTILINE | re.DOTALL)

reOtherNamespace = re.compile("^(User|Wikipedia|File|MediaWiki|Template|Help|Category|Portal|Book|Talk|Special|Media|WP|User talk|Wikipedia talk|File talk|MediaWiki talk|Template talk|Help talk|Category talk|Portal talk):.+",re.DOTALL)

if FORMAT == 'Zemanta-modern':
	rePage = rePageModern
else:
	rePage = rePageLegacy

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


RSIZE = 10000000	# read chunk size = 10 MB

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
def recordArticle(pageDict):
   global articleBuffer, textBuffer, aBuflen, STEMMER

   if FORMAT == 'Zemanta-modern' and (pageDict['disambig'] == '1' or pageDict['cat'] == '1' or pageDict['img'] == '1'):
	return

   # a simple check for content
   if int(pageDict['len']) < 10:
	return

   mContent = reContent.search(pageDict['page'])
   if not mContent:
	return

   contentDict = mContent.groupdict()

   title = contentDict['title']

   # only keep articles of Main namespace
   if reOtherNamespace.match(title):
        return

   id = int(pageDict['id'])

   # skip disambig   
   if FORMAT != 'Zemanta-modern' and id in DISAMBIG_IDS:
	return

   # ** stop category filter **
   cs = contentDict['categories']
   cs = cs.split()
   cats = frozenset([int(c) for c in cs if c])

   # filter article with no category or belonging to stop categories
   if not cats or STOP_CATS.intersection(cats):
        log.write('Filtered concept id='+str(id)+' ('+ title +') [stop category]\n')
	return
   # ******

   # ** title filter **
   if piped_re.match(title):
       log.write('Filtered concept id='+str(id)+' ('+ title +') [regex]\n')
       return

   '''if reList.match(title):
       log.write('Filtered concept id='+str(id)+' ('+ title +') [list]\n')
       return'''
   # ******

   # ** inlink-outlink filter **
   if not inlinkDict.has_key(id) or inlinkDict[id] < 5:
        log.write('Filtered concept id='+str(id)+' ('+ title +') [minIncomingLinks]\n')
	return

   if not outlinkDict.has_key(id) or outlinkDict[id] < 5:
        log.write('Filtered concept id='+str(id)+' ('+ title +') [minOutgoingLinks]\n')
	return
   # ******

   text = contentDict['text']

   # convert HTML to plain text
   t = html.fromstring(title.decode("utf-8"))
   ctitle = t.text_content()

   ctext = '' 
   t = html.fromstring(text.decode("utf-8"))
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
        log.write('Filtered concept id='+str(id)+' ('+ title +') [minNumFeaturesPerArticle]\n')
	return


   cadd = ''
   for i in range(TITLE_WEIGHT):
	cadd += ctitle + ' \n '
   cadd += ctext

   # write article info (id,title,text)
   articleBuffer.append((id,ctitle))
   textBuffer.append((id,cadd))
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
prevText = ''

firstRead = f.read(10000)

if FORMAT == 'Gabrilovich':
	documentStart = firstRead.find('</siteinfo>') + len('</siteinfo>')
else:
	documentStart = firstRead.find('<gum>') + len('<gum>')

prevText = firstRead[documentStart:10000]

while True:

    newText = f.read(RSIZE)
    if not newText:
        break
    
    text = prevText + newText

    endIndex = -1
    
    for page in rePage.finditer(text):
        recordArticle(page.groupdict())
	endIndex = page.end()

    prevText = text[endIndex:]

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

