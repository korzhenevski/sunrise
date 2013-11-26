#!/usr/bin/env python

import json, socket, itertools, re, sys
from pprint import pprint as pp
from zlib import crc32


class Client(object):
    def __init__(self, addr):
        self.socket = socket.create_connection(addr)
        self.id_counter = itertools.count()


    def call(self, name, *params):
        request = dict(id=next(self.id_counter),
                    params=list(params),
                    method=name)
        self.socket.sendall(json.dumps(request).encode())

        # This must loop if resp is bigger than 4K
        response = self.socket.recv(4096)
        response = json.loads(response.decode())

        if response.get('id') != request.get('id'):
            raise Exception("expected id=%s, received id=%s: %s"
                            %(request.get('id'), response.get('id'),
                              response.get('error')))

        if response.get('error') is not None:
            raise Exception(response.get('error'))

        return response.get('result')

"""
c = Client(('localhost', 4242))

from pymongo import MongoClient
db = MongoClient()['test']

for stream in db.ss.find():
    task = {
        'StreamId': stream['_id'],
        'StreamUrl': stream['url'],
        'ServerId': 101,
        'Record': True,
        'RecordDuration': 3600,
        'MinRetryInterval': 10,
        'MaxRetryInterval': 3600,
        'UserAgent': "robot/1.0",   
    }
    res = c.call("Tracker.PutTask", task)
    db.ss.update({'_id': stream['_id']}, {'$set': {'queue_id': res['QueueId']}})
    pp(res)
"""


class StreamControl(object):
    def __init__(self, manager_addr):
        self.client = Client(manager_addr)

    def put_stream(self, url, stream_id, server_id, user_agent=None, record=False, record_duration=600, retry=10, max_retry=3600, **kwargs):
        task = {
            'StreamId': stream_id,
            'StreamUrl': stream_url,
            'ServerId': int(server_id),
            'Record': bool(record),
            'RecordDuration': int(record_duration),
            'MinRetryInterval': int(retry),
            'MaxRetryInterval': int(max_retry),
            'UserAgent': user_agent,
        }        
        return self.client.call("Tracker.PutTask", task)
        
import argparse

parser = argparse.ArgumentParser(
        description='Make a golang jsonrpc call to a remote service.'
        )

parser.add_argument('--addr', help='specify address to connect to.', default='localhost:4242')
parser.add_argument('--urls', help='urls file')
parser.add_argument('command', nargs='?', help='remote procedure to call')
parser.add_argument('params', nargs='*', help='parameters for the remote call')

#c = StreamControl(('localhost', 4242))
#c.put_stream(url)

def to_camelcase(s):
    return re.sub(r'(?!^)_([a-zA-Z])', lambda m: m.group(1).upper(), s)

def safe_json_loads(raw):
    try:
        return json.loads(raw)
    except ValueError:
        return raw

def main2():
    args = parser.parse_args()

    host, port = args.addr.split(':')
    c = Client((host, int(port)))

    params = [param.split('=', 1) for param in args.params]
    params = dict(( (to_camelcase(k), safe_json_loads(v)) for k, v in params ))

    pp((args.command, params))    
    pp(c.call(to_camelcase(args.command), params))

def main():
    args = parser.parse_args()
    print args

    host, port = args.addr.split(':')
    c = Client((host, int(port)))

    f = open(args.urls)

    sid = 1
    for i in f:
        sid += 1
	url = i.strip() 
        task = {
            'StreamId': crc32(url) & 0xFFFFFFFF,
            'StreamUrl': url,
            'ServerId': 1,
	    'Record': True,
            #'Record': True if sid < 15 else False,
            'RecordDuration': 3600,
            'MinRetryInterval': 10,
            'MaxRetryInterval': 3600,
            'UserAgent': "",
        }

        pp(task)
        pp(c.call("Tracker.PutTask", task))

if __name__ == '__main__':
    main()
