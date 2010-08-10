#!/usr/bin/python

'''
Copyright (C) 2010  Cagatay Calli <ccalli@gmail.com>

Adds redirections from Wikiprep output to target Wikipedia articles.

USAGE: addRedirects.py <redir file from Wikiprep> <any writeable folder>
The folder is used by the script to create data files that are loaded into database.

IMPORTANT: If you use XML output from a recent version of Wikiprep
(e.g. Zemanta fork), then set FORMAT to 'Zemanta-legacy' or 'Zemanta-modern'.
'''

import sys
import re
import MySQLdb

# formats: 1) Gabrilovich 2) Zemanta-legacy 3) Zemanta-modern
FORMAT = 'Gabrilovich'	

PARTITION_SIZE = 100000

RSIZE = 10000000        # read chunk size = 10 MB - implicit for now


reModernREDIR = re.compile('<redirect>\n<from>\n<id>.+?</id>\n<name>(?P<text>.+?)</name>\n</from>\n<to>\n<id>(?P<target>\d+)</id>\n<name>.+?</name>\n</to>\n</redirect>',re.DOTALL|re.MULTILINE)

reLegacyREDIR = re.compile('<redirect>\n<from>\n<id>.+?</id>\n<title>(?P<text>.+?)</title>.*?</from>\n<to>\n<id>(?P<target>\d+)</id>\n<title>.+?</title>\n</to>\n</redirect>',re.DOTALL|re.MULTILINE)

if 'Zemanta-modern':
	reREDIR = reModernREDIR
else:
	reREDIR = reLegacyREDIR

args = sys.argv[1:]

if len(args) < 2:
        sys.exit(1)

f = open(args[0],'r')

outFolder = args[1].rstrip('/') + '/'
outPrefix = outFolder + '/zredir'

out = open(outPrefix + '0','w')

try:
	conn = MySQLdb.connect(host='localhost',user='root',passwd='123456',db='wiki',charset = "utf8", use_unicode = True)
except MySQLdb.Error, e:
	print "Error %d: %s" % (e.args[0], e.args[1])
	sys.exit(1)

lc = 0
outk = 0

prevText = ''

firstRead = f.read(10000)

documentStart = firstRead.find('<redirects>') + len('<redirects>')
prevText = firstRead[documentStart:10000]


while True:

    newText = f.read(RSIZE)
    if not newText:
        break

    text = prevText + newText

    endIndex = -1

    for page in reREDIR.finditer(text):
	out.write(page.group(2) + '\t' + page.group(1) + '\n')
	lc += 1

        if lc >= PARTITION_SIZE:
                lc = 0
                outk += 1
                out.close()
                out = open(outPrefix + str(outk),'w')

        endIndex = page.end()

    prevText = text[endIndex:]

f.close()

if lc > 0:
	out.close()

outk += 1


try:
        cursor = conn.cursor()

        for i in range(outk):
                si = str(i)
                cursor.execute("DROP TABLE IF EXISTS zredir"+si)
                cursor.execute("CREATE TABLE zredir"+si+" (target_id int(10) unsigned, redir varbinary(255));")
                cursor.execute("LOAD DATA LOCAL INFILE '"+outPrefix+si+"' INTO TABLE zredir"+si)
                cursor.execute("CREATE INDEX idx_target_id ON zredir"+si+" (target_id);")

                cursor.execute("DROP TABLE IF EXISTS redirList"+si)
                cursor.execute("CREATE TABLE redirList"+si+" SELECT a.target_id,GROUP_CONCAT(a.redir SEPARATOR ' \n') AS redir_text FROM zredir"+si+" a WHERE a.redir IS NOT NULL GROUP BY a.target_id;")

                cursor.execute("DROP TABLE zredir"+si)

                # add redirects after creating each partition
                cursor.execute("CREATE INDEX idx_target_id ON redirList"+si+" (target_id);")
                cursor.execute("UPDATE text t, redirList"+si+" a SET t.old_text = CONCAT(a.redir_text,' \n',t.old_text) WHERE t.old_id = a.target_id AND a.redir_text IS NOT NULL;")
                cursor.execute("DROP TABLE redirList"+si)

        cursor.close()
        conn.close()
except MySQLdb.Error, e:
        print "Error %d: %s" % (e.args[0], e.args[1])
        sys.exit (1)
