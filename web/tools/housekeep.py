import pymongo
import requests

db = pymongo.Connection()['test']

for rec in db.records.find({'end': True}, sort=[('ts', 1)]).limit(30000):
    tid = rec['track_id']
    print tid
    track = db.air.find_one({'_id': tid})
    print track
    resp = requests.delete('http://127.0.0.1:9200/sunrise/track/%d' % tid)
    info= resp.json()
    print info
    print db.air.remove({'_id': tid})
    print db.records.remove({'track_id': tid})
    if track:
        db.rm_air.insert(track)
    db.rm_records.insert(rec)
