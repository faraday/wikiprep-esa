#!/usr/bin/python

'''
Copyright (C) 2010  Cagatay Calli <ccalli@gmail.com>

Adds anchors from Wikiprep output to target Wikipedia articles.

Legacy input format: <Target page id>  <Source page id>  <Anchor text (up to the end of the line)>
Input format: <Target page id>  <Source page id>  <Anchor location within text>  <Anchor text (up to the end of the line)>
Output format: <Target page id>	<Anchor text>

USAGE: addAnchors.py <anchor file from Wikiprep> <any writeable folder>
The folder is used by the script to create data files that are loaded into database.

IMPORTANT: If you use XML output from a recent version of Wikiprep
(e.g. Zemanta fork), then set LEGACY_MODE = False.
'''

import sys
import MySQLdb

PARTITION_SIZE = 10000000

LEGACY_MODE = True

if LEGACY_MODE:
	FIELD_POS = 2
else:
	FIELD_POS = 3

args = sys.argv[1:]

if len(args) < 2:
        sys.exit(1)

f = open(args[0],'r')

outFolder = args[1].rstrip('/') + '/'
outPrefix = outFolder + '/zanchor'

out = open(outPrefix + '0','w')

for i in range(3):
	f.readline()

lc = 0
outk = 0

for line in f.xreadlines():
	fields = line.split('\t')
	anc = fields[FIELD_POS].rstrip('\n')
	out.write(fields[0] + '\t' + anc + '\n')
	lc += 1

	if lc >= PARTITION_SIZE:
		lc = 0
		outk += 1
		out.close()
		out = open(outPrefix + str(outk),'w')

out.close()
f.close()

if lc > 0:
	out.close()

outk += 1

try:
        conn = MySQLdb.connect(host='localhost',user='root',passwd='123456',db='wiki',charset = "utf8", use_unicode = True)
except MySQLdb.Error, e:
        print "Error %d: %s" % (e.args[0], e.args[1])
        sys.exit(1)

try:
        cursor = conn.cursor()

	for i in range(outk):
		si = str(i)
		cursor.execute("DROP TABLE IF EXISTS zanchor"+si)
        	cursor.execute("CREATE TABLE zanchor"+si+" (target_id int(10) unsigned, anchor varbinary(255))")
		cursor.execute("LOAD DATA LOCAL INFILE '"+outPrefix+si+"' INTO TABLE zanchor"+si)
		cursor.execute("CREATE INDEX idx_target_id ON zanchor"+si+" (target_id);")

		cursor.execute("DROP TABLE IF EXISTS anchorList"+si)
		cursor.execute("CREATE TABLE anchorList"+si+" (target_id int(10) unsigned, anchor_text mediumblob)")
	        cursor.execute("INSERT anchorList"+si+" SELECT a.target_id,GROUP_CONCAT(a.anchor SEPARATOR ' \n ') AS anchor_text FROM zanchor"+si+" a WHERE a.anchor IS NOT NULL GROUP BY a.target_id")

		cursor.execute("DROP TABLE zanchor"+si)

		# add anchors after creating each partition
		cursor.execute("CREATE INDEX idx_target_id ON anchorList"+si+" (target_id);")
		cursor.execute("UPDATE text t, anchorList"+si+" a SET t.old_text = CONCAT(a.anchor_text,' \n',t.old_text) WHERE t.old_id = a.target_id AND a.anchor_text IS NOT NULL;")
		cursor.execute("DROP TABLE anchorList"+si)

        cursor.close()
        conn.close()
except MySQLdb.Error, e:
        print "Error %d: %s" % (e.args[0], e.args[1])
        sys.exit (1)


