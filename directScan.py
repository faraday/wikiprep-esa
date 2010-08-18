#!/usr/bin/python

'''
Copyright (C) 2010  Cagatay Calli <ccalli@gmail.com>

Run scanLinks.py first..
Scans using IDs from Gabrilovich log and XML output (gum.xml) from Wikiprep, creates 3 tables:

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

TITLE_WEIGHT = 4

idList = []
try:
	f = open('selected.txt','r')
	for line in f.readlines():
		strId = line.split('\t')[0]
		if strId:
			idList.append(int(strId))
	f.close()
except:
	print '(Direct) Article list cannot be read! Please put "selected.txt" file containing stop categories in this folder.'
	sys.exit(1)

ARTICLE_IDS = frozenset(idList)

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

reContent = re.compile('<title>(?P<title>.+?)</title>\n<categories>(?P<categories>.*?)</categories>\n<links>(?P<links>.*?)</links>.+?<text>(?P<text>.+?)</text>',re.MULTILINE | re.DOTALL)

if FORMAT == 'Zemanta-modern':
	rePage = rePageModern
else:
	rePage = rePageLegacy

RSIZE = 10000000	# read chunk size = 10 MB

###
articleBuffer = []	# len: 100  / now: 200
aBuflen = 0

textBuffer = []		# same as articleBuffer, stores text
###

# pageContent - <page>..content..</page>
# pageDict - stores page attribute dict
def recordArticle(pageDict):
   global articleBuffer, textBuffer, aBuflen

   '''if FORMAT == 'Zemanta-modern' and (pageDict['stub'] == '1' or pageDict['disambig'] == '1' or pageDict['cat'] == '1' or pageDict['img'] == '1'):
	return
   elif FORMAT != 'Zemanta-modern' pageDict['stub'] == '1':
	return'''

   id = int(pageDict['id'])
   if not id in ARTICLE_IDS:
	return

   mContent = reContent.search(pageDict['page'])
   contentDict = mContent.groupdict()

   title = contentDict['title']
   text = contentDict['text']

   # convert HTML to plain text
   t = html.fromstring(title.decode("utf-8"))
   ctitle = t.text_content()

   ctext = '' 
   t = html.fromstring(text.decode("utf-8"))
   ctext = t.text_content()

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

