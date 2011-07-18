import xml.etree.cElementTree as cElementTree
import sys
import string

"""
From project: wikiprep-postprocess
https://github.com/turian/wikiprep-postprocess

written by Joseph Turian
"""

def read(f):
    """
    Generator for reading a wikiprep XML file from a file object.
    """
    print >> sys.stderr, "Reading %s..." % f
    # print >> sys.stderr, stats()
    doc = {}
    cnt = 0
    for event, elem in cElementTree.iterparse(f):
        if elem.tag == "title":
            doc["title"] = ("".join(elem.itertext()))
        elif elem.tag == "text":
            doc["text"] = ("".join(elem.itertext()))
        elif elem.tag == "link":
            # Skip internal links
            if elem.get("url") is None: continue

            if "external links" not in doc: doc["external links"] = []
            doc["external links"].append([elem.get("url"), ("".join(elem.itertext()))])
        elif elem.tag == "links":
            doc["links"] = [int(i) for i in string.split("".join(elem.itertext()))]
        elif elem.tag == "categories":
            doc["categories"] = [int(i) for i in string.split("".join(elem.itertext()))]
        elif elem.tag == "page":
            doc["_id"] = int(elem.get("id"))
	    doc["length"] = int(elem.get("newlength"))
	    if elem.get("stub"):
		doc["stub"] = bool(elem.get("stub")=="1")
	    if elem.get("disambig"):
		doc["disambig"] = bool(elem.get("disambig")=="1")
	    if elem.get("image"):
		doc["image"] = bool(elem.get("image")=="1")
	    if elem.get("category"):
		doc["category"] = bool(elem.get("category")=="1")

            cnt += 1
            yield doc
            doc = {}

            # Free the memory of the building tree
            elem.clear()
            if cnt % 1000 == 0:
                print >> sys.stderr, "Read %d articles from %s" % (cnt, f)
                # print >> sys.stderr, stats()
    print >> sys.stderr, "...done reading %s" % f
    #print >> sys.stderr, stats()
