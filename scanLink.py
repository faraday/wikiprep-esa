#!/usr/bin/python

'''
Copyright (C) 2010  Cagatay Calli <ccalli@gmail.com>

Scans outgoing links for a target Wikipedia page.

USAGE: scanLink.py <target page id>
The script connects to the database, retrieves the outgoing links
for the requested page and prints the target page ids to standard output.
'''

import sys
import MySQLdb

try:
        conn = MySQLdb.connect(host='localhost',user='root',passwd='123456',db='wiki',charset = "utf8", use_unicode = True)
except MySQLdb.Error, e:
        print "Error %d: %s" % (e.args[0], e.args[1])
        sys.exit(1)

if len(sys.argv) < 2:
        sys.exit(1)

try:
        # retrieve outgoing links for the requested page
        _id = int(sys.argv[1])
        cursor = conn.cursor()
        cursor.execute("SELECT target_id FROM pagelinks WHERE source_id = %s",(_id,))
        for row in cursor.fetchall():
            print row[0]
        cursor.close()

        conn.close()
except MySQLdb.Error, e:
        print "Error %d: %s" % (e.args[0], e.args[1])
        sys.exit(1)
