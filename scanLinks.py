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

LINK_LOAD_THRES = 100000

# formats: 1) Gabrilovich 2) Zemanta-legacy 3) Zemanta-modern
FORMAT = 'Gabrilovich'

try:
	conn = MySQLdb.connect(host='localhost',user='root',passwd='123456',db='wiki',charset = "utf8", use_unicode = True)
except MySQLdb.Error, e:
	print "Error %d: %s" % (e.args[0], e.args[1])
	sys.exit(1)

try:
	cursor = conn.cursor()

	cursor.execute("DROP TABLE IF EXISTS namespace")
	cursor.execute("""
		CREATE TABLE namespace
		(
		  id INT(10),
		  KEY (id)
		) DEFAULT CHARSET=binary
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

rePageLegacy = re.compile('<page id="(?P<id>\d+)".+?newlength="(?P<len>\d+)".+?>(?P<page>.+?)</page>',re.MULTILINE | re.DOTALL)

rePageModern = re.compile('<page id="(?P<id>\d+)".+?newlength="(?P<len>\d+)".+?>(?P<page>.+?)</page>',re.MULTILINE | re.DOTALL)

reContent = re.compile('<title>(?P<title>.+?)</title>.+?<links>(?P<links>.*?)</links>',re.MULTILINE | re.DOTALL)

reOtherNamespace = re.compile("^(User|Wikipedia|File|MediaWiki|Template|Help|Category|Portal|Book|Talk|Special|Media|WP|User talk|Wikipedia talk|File talk|MediaWiki talk|Template talk|Help talk|Category talk|Portal talk):.+",re.DOTALL)

if FORMAT == 'Zemanta-modern':
	rePage = rePageModern
else:
	rePage = rePageLegacy

RSIZE = 10000000	# read chunk size = 10 MB

linkBuffer = []
linkBuflen = 0

nsBuffer = []
nsBuflen = 0

mainNS = []

# pageContent - <page>..content..</page>
# pageDict - stores page attribute dict
def recordArticle(pageDict):
   global linkBuffer, linkBuflen, nsBuffer, nsBuflen

   # a simple check for content
   if int(pageDict['len']) < 10:
	return

   mContent = reContent.search(pageDict['page'])
   if not mContent:
	return

   contentDict = mContent.groupdict()

   id = int(pageDict['id'])
   title = contentDict['title']

   # only keep articles of Main namespace
   if reOtherNamespace.match(title):
        return

   nsBuffer.append((id))
   nsBuflen += 1

   if linkBuflen >= 10000:
   	cursor.executemany("""
		INSERT INTO namespace (id)
                VALUES (%s)
              	""",nsBuffer)

        nsBuffer = []
        nsBuflen = 0


   ls = contentDict['links']
   ls = ls.split()

   # write links
   for l in ls:
        linkBuffer.append((id,l)) # source, target
        linkBuflen += 1

        if linkBuflen >= 10000:
                cursor.executemany("""
                        INSERT INTO pagelinks (source_id,target_id)
                        VALUES (%s,%s)
                        """,linkBuffer)

                linkBuffer = []
                linkBuflen = 0

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

if nsBuflen > 0:
    cursor.executemany("""
	INSERT INTO namespace (id)
        VALUES (%s)
        """,nsBuffer)

    nsBuffer = []
    nsBuflen = 0

if linkBuflen > 0:
    cursor.executemany("""
	INSERT INTO pagelinks (source_id,target_id)
        VALUES (%s,%s)
        """,linkBuffer)

    linkBuffer = []
    linkBuflen = 0


cursor.execute("DROP TABLE IF EXISTS tmppagelinks")
cursor.execute("CREATE TABLE tmppagelinks LIKE pagelinks")
cursor.execute("INSERT tmppagelinks SELECT p.* FROM pagelinks p WHERE EXISTS (SELECT * FROM namespace n WHERE p.target_id = n.id)")
cursor.execute("DROP TABLE pagelinks")
cursor.execute("RENAME TABLE tmppagelinks TO pagelinks")

# inlinks
cursor.execute("DROP TABLE IF EXISTS inlinks")
cursor.execute("CREATE TABLE inlinks AS SELECT p.target_id, COUNT(p.source_id) AS inlink FROM pagelinks p GROUP BY p.target_id")
cursor.execute("CREATE INDEX idx_target_id ON inlinks (target_id)")

# outlinks
cursor.execute("DROP TABLE IF EXISTS outlinks")
cursor.execute("CREATE TABLE outlinks AS SELECT p.source_id, COUNT(p.target_id) AS outlink FROM pagelinks p GROUP BY p.source_id")
cursor.execute("CREATE INDEX idx_source_id ON outlinks (source_id)")

cursor.close()
conn.close()
