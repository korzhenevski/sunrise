import pymongo

db = pymongo.Connection()['test']

for rec in db.rm_records.find():
    print rec['path']
