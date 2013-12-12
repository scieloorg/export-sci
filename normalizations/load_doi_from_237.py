from pymongo import Connection

conn = Connection('192.168.1.76', 27017)
db = conn['scielo_network']
coll = db['articles']
coll.ensure_index('code')
coll.ensure_index('article.doi')

pids = coll.find({'article.v237': {'$exists': True}}, {'code': 1, 'article.v237':1})
for reg in pids:
    coll.update({'code': reg['code']}, {'$set': {'article.doi': reg['article']['v237'][0]['_']}})
