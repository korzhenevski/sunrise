import pymongo
import random

db = pymongo.Connection()['test']
limit = 128 << 20

def get_id(ns):
    ret = db.ids.find_and_modify({'_id': ns}, {'$inc': {'next': 1}}, new=True, upsert=True)
    return ret['next']

def new_record():
    vol = random.choice(list(db.volumes.find({'upload': True, 'server_id': 2})))
    rid = get_id('records') + 1000000

    record = {"_id": rid, "vol_id": vol['_id']}

    db.records.insert(record)
    return record, vol

def record_path(vol_path, record_id):
    return '%s/%s/%s.mp3' % (vol_path, str(record_id)[-1:], record_id)

while True:
    offset = 0
    first = True
    a = db.air.find_one({'offset': {'$exists': False}, 'rid': {'$gt': 0}}, sort=[('_id', 1)])
    if not a:
        break
    print a['_id'], 'from stream', a['stream_id']
    record, vol = new_record()
    #print 'new record', record['_id']

    while a:
        old_record = db.records.find_one({'track_id': a['_id']})
        if (old_record['size'] + offset) > limit and not first:
            db.records.update({'_id': record['_id']},{'$set':{ 'size': offset}})
            record, vol = new_record()
            offset = 0
     	#       print 'new record', record['_id']
       
        #print 'merge', old_record['path'], record_path(vol['path'], record['_id']), offset
        print old_record['path'], record_path(vol['path'], record['_id']), offset
 
        db.air.update({'_id': a['_id']}, {'$set': {'record_id': record['_id'], 'size': old_record['size'], 'offset': offset}})
        offset += old_record['size']
        a = db.air.find_one({'_id': {'$gt': a['_id']}, 'offset': {'$exists': False}, 'stream_id': a['stream_id'], 'rid': {'$gt': 0}}, sort=[('_id', 1)])
        #db.records.remove({'_id': old_record['_id']})
        #print 'next', a['_id'], a['stream_id']
        first = False
    break
