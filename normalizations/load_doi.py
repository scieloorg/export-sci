from pymongo import Connection

conn = Connection('192.168.1.76', 27017)
db = conn['scielo_network']
coll = db['articles']
coll.ensure_index('code')

pids_doi = {}
with open('dois.txt') as f:

    for line in f:
        splited = line.split('|')
        if not len(splited) == 2:
            continue

        if not len(splited[0]) == 23:
            continue
        
        pids_doi[splited[0]] = splited[1].strip()

regs = coll.find({'article.doi': {'$exists': 0}}, {'code': 1})

for reg in regs:
    if reg['code'] in pids_doi:
        print 'including doi for %s' % reg['code']
        coll.update({'code': reg['code']}, {'$set': {'article.doi': pids_doi[reg['code']]}})
