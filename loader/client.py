#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json, socket, itertools, re, sys, time
from pprint import pprint as pp

class Client(object):
    def __init__(self, addr):
        self.addr = addr 
        self.id_counter = itertools.count()

    def call(self, name, *params):
        request = dict(id=next(self.id_counter),
                    params=list(params),
                    method=name)
        sock = socket.create_connection(self.addr)
        sock.sendall(json.dumps(request).encode())

        # This must loop if resp is bigger than 64K
        response = sock.recv(1024 * 64)
        response = json.loads(response.decode())

        if response.get('id') != request.get('id'):
            raise Exception("expected id=%s, received id=%s: %s"
                            %(request.get('id'), response.get('id'),
                              response.get('error')))

        if response.get('error') is not None:
            raise Exception(response.get('error'))

        return response.get('result')

class SimpleClient(Client):
    debug = False

    def __call__(self, fn, *args, **kw):
        # convert radio_get => Radio.Get
        fn = fn.title().replace('_', '.', 1).replace('_', '')
    
        # convert key_name => KeyName
        params = dict((k.title().replace('_', ''), v) for k, v in kw.iteritems())
    
        ts = time.time()
        res = self.call(fn, params)
        if self.debug:
            latency = round(time.time() - ts, 4)
            pp((latency, fn, params, res))

    	return res

    def __getattr__(self, method):
        return lambda *args, **kargs: self(method, *args, **kargs)