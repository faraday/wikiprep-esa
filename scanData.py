#!/usr/bin/python

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

# formats: 1) Gabrilovich 2) Zemanta-legacy 3) Zemanta-modern
FORMAT = 'Gabrilovich'

reToken = re.compile('[a-zA-Z]+')
NONSTOP_THRES = 100

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
	f = open('extended_stop_categories.txt','r')
	for line in f.readlines():
		strId = line.strip()
		catList.append(strId)
	f.close()
except:
	print 'Stop categories cannot be read! Please put "extended_stop_categories.txt" file containing stop categories in this folder.'
	sys.exit(1)

STOP_CATS = frozenset(catList)

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

	cursor.execute("DROP TABLE IF EXISTS pagelinks")
	cursor.execute("""
		CREATE TABLE pagelinks
		(
		  source_id INT(10),
		  target_id INT(10),
		  KEY (source_id),
		  KEY (target_id)
		) DEFAULT CHARSET=binary
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

reContent = re.compile('<title>(?P<title>.+?)</title>\n<categories>(?P<categories>.*?)</categories>\n<links>(?P<links>.*?)</links>.+?<text>(?P<text>.+?)</text>',re.MULTILINE | re.DOTALL)

reOtherNamespace = re.compile("^(User|Wikipedia|File|MediaWiki|Template|Help|Category|Portal|Book):.+",re.DOTALL)

if FORMAT == 'Zemanta-modern':
	rePage = rePageModern
else:
	rePage = rePageLegacy

# category, disambig, stub pages are removed by flags

# regex as article filter (dates, lists, etc.)
re_strings = ['^(January|February|March|April|May|June|July|August|September|October|November|December) \d+$',
	      '^\d+((st|nd|th) (century|millenium))?( (AD|BC|AH|BH|AP|BP))?( in [^$]+)?$',
	      '^List of .+',
	      '.+\(disambiguation\)']
piped_re = re.compile( "|".join( re_strings ) , re.DOTALL|re.IGNORECASE)


RSIZE = 10000000	# read chunk size = 10 MB

###
articleBuffer = []	# len: 100  / now: 200
aBuflen = 0

textBuffer = []		# same as articleBuffer, stores text

linkBuffer = []		# len: 10000
linkBuflen = 0 
###

# pageContent - <page>..content..</page>
# pageDict - stores page attribute dict
def recordArticle(pageDict):
   global articleBuffer, linkBuffer, textBuffer, aBuflen, linkBuflen

   '''if FORMAT == 'Zemanta-modern' and (pageDict['stub'] == '1' or pageDict['disambig'] == '1' or pageDict['cat'] == '1' or pageDict['img'] == '1'):
	return
   elif FORMAT != 'Zemanta-modern' pageDict['stub'] == '1':
	return'''

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

   # filter articles based on title  
   if piped_re.match(title):
       return

   text = contentDict['text']
   cats = contentDict['categories']
   cats = cats.split()
   links = contentDict['links']
   links = links.split()

   # filter article with no category or belonging to stop categories
   if not cats or STOP_CATS.intersection(set(cats)):
	return

   # filter articles with outlinks < 5
   if len(links) < 5:
	return

   # convert HTML to plain text
   t = html.fromstring(title.decode("utf-8"))
   ctitle = t.text_content()

   ctext = '' 
   t = html.fromstring(text.decode("utf-8"))
   ctext = t.text_content()

   # filter articles with fewer than 100 non-stop words
   wordCount = NONSTOP_THRES
   for m in reToken.finditer(ctext):
	w = m.group()
	if w and not (len(w) < 3 or w in STOP_WORDS):
		wordCount -= 1
		if wordCount == 0:
			break

   if wordCount > 0:
	return

   # write links
   for l in links:
	linkBuffer.append((id,l)) # source, target
	linkBuflen += 1

   	if linkBuflen >= 10000:
		cursor.executemany("""
			INSERT INTO pagelinks (source_id,target_id)
			VALUES (%s,%s)
			""",linkBuffer)

		linkBuffer = []
		linkBuflen = 0

   # convert HTML to plain text
   '''text = title + " \n " + title + " \n " + text
   t = html.fromstring(text.decode("utf-8"))
   text = t.text_content()'''

   # write article info (id,title,text)
   articleBuffer.append((id,ctitle))
   textBuffer.append((id,ctext))
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


args = sys.argv[1:]
# scanData.py <hgw_file> <RSIZE>

if len(args) < 1:
    sys.exit()

if len(args) == 2:
    RSIZE = int(args[1])

f = open(args[0],'r')
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

# last writes
if linkBuflen > 0:
	cursor.executemany("""
		INSERT INTO pagelinks (source_id,target_id)
		VALUES (%s,%s)
		""",linkBuffer)

	linkBuffer = []


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

# remove article with inlink < 5 , outlinks < 5

# inlinks
cursor.execute("DROP TABLE IF EXISTS inlinks")
cursor.execute("CREATE TABLE inlinks AS SELECT p.target_id, COUNT(p.source_id) AS inlink FROM pagelinks p GROUP BY p.target_id")
cursor.execute("CREATE INDEX idx_target_id ON inlinks (inlink)")

# filter
cursor.execute("CREATE TABLE tmparticle LIKE article")
cursor.execute("INSERT tmparticle SELECT a.* FROM article a, inlinks i WHERE a.id = i.target_id AND i.inlink > 5")
cursor.execute("DROP TABLE article")
cursor.execute("RENAME TABLE tmparticle TO article")

#cursor.execute("DROP TABLE inlinks")

# remove links to articles that are filtered out
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

