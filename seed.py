import pymongo


db = pymongo.Connection()['test']

for i in xrange(100):
	print db.queue.insert({'_id': i, 'server_id': 101, 'url': 'http://example.com', 'ts': 0})
