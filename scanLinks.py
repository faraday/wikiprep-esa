#!/usr/bin/python

'''
Copyright (C) 2010  Cagatay Calli <ccalli@gmail.com>

Scans XML output (gum.xml) from Wikiprep, creates 3 tables:

TABLE: pagelinks	COLUMNS: source_id INT, target_id INT
TABLE: inlinks		COLUMNS: target_id INT, inlink INT
TABLE: outlinks		COLUMNS: source_id INT, outlink INT
TABLE: namespace	COLUMNS: id INT

USAGE: scanData.py <hgw.xml file from Wikiprep>

IMPORTANT: If you use XML output from a recent version of Wikiprep
(e.g. Zemanta fork), then set FORMAT to 'Zemanta-legacy' or 'Zemanta-modern'.

'''

import sys
import re
import MySQLdb
import signal
import xmlwikiprep

LINK_LOAD_THRES = 10000

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

reOtherNamespace = re.compile("^(User|Wikipedia|File|MediaWiki|Template|Help|Category|Portal|Book|Talk|Special|Media|WP|User talk|Wikipedia talk|File talk|MediaWiki talk|Template talk|Help talk|Category talk|Portal talk):.+",re.DOTALL)

linkBuffer = []
linkBuflen = 0

nsBuffer = []
nsBuflen = 0

mainNS = []

# pageContent - <page>..content..</page>
# pageDict - stores page attribute dict
def recordArticle(pageDoc):
   global linkBuffer, linkBuflen, nsBuffer, nsBuflen

   # a simple check for content
   if pageDoc['length'] < 10:
	return

   _id = pageDoc['_id']

   # only keep articles of Main namespace
   if reOtherNamespace.match(pageDoc['title']):
        return

   nsBuffer.append((_id))
   nsBuflen += 1

   if linkBuflen >= LINK_LOAD_THRES:
   	cursor.executemany("""
		INSERT INTO namespace (id)
                VALUES (%s)
              	""",nsBuffer)

        nsBuffer = []
        nsBuflen = 0


   # write links
   for l in pageDoc['links']:
        linkBuffer.append((_id,l)) # source, target
        linkBuflen += 1

        if linkBuflen >= LINK_LOAD_THRES:
                cursor.executemany("""
                        INSERT INTO pagelinks (source_id,target_id)
                        VALUES (%s,%s)
                        """,linkBuffer)

                linkBuffer = []
                linkBuflen = 0

   return


args = sys.argv[1:]
# scanData.py <hgw_file>

if len(args) < 1:
    sys.exit()

f = open(args[0],'r')
for doc in xmlwikiprep.read(f):
	recordArticle(doc)
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
