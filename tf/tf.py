#!/usr/bin/python
# -*- coding: utf-8 -*-
import gevent
from gevent.monkey import patch_all
patch_all()
import requests
import pymongo
import time
from pprint import pprint as pp

LASTFM = "http://ws.audioscrobbler.com/2.0/?api_key=048cd62229f507d6d577393a6d7ac972&format=json"

def get_track_info():
	resp = requests.get(LASTFM, params=dict(method='track.getInfo', autocorrect=1, artist='cher', track='believe'))
	pp(resp)

def main():
	db = pymongo.MongoClient()['test']
	last_id = None
	while True:
		where = {"_id": {"$gt": last_id}} if last_id else {}
		cursor = db.log.find(where, tailable=True, await_data=True)
		while cursor.alive:
			try:
				record = cursor.next()
				last_id = record['_id']
				print record
			except StopIteration:
				break
		time.sleep(5)

if __name__ == '__main__':
	main()