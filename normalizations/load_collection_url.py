import urllib2
import json
from urlparse import urlparse
from pymongo import Connection

json_journals = json.loads(urllib2.urlopen("http://webservices.scielo.org/scieloorg/_design/couchdb/_view/title?limit=2000").read())

def load_journals_urls():    

    journals = {}
    for reg in json_journals['rows']:
        url = urlparse(reg['value']['url']).netloc
        journals.setdefault(reg['value']['issn'], url)

    return journals

def load_journals_collections():

    journals = {}
    for reg in json_journals['rows']:
        collection = reg['value']['collection']
        journals.setdefault(reg['value']['issn'], collection)

    return journals

urls = load_journals_urls()
collections = load_journals_collections()

conn = Connection('192.168.1.76', 27017)
db = conn['scielo_network']
coll = db['articles']
coll.ensure_index('code')
regs = coll.find({'collection': {'$exists': 0}}, {'code': 1, 'title.v690': 1})

for reg in regs:
    issn = reg['code'][1:10].upper()
    if issn in urls:
        coll.update({'code': reg['code']}, {'$set': {'title.v690': [{'_': urls[issn]}]}}, True)
    if issn in collections:
        coll.update({'code': reg['code']}, {'$set': {'collection': collection[issn]}}, True)
