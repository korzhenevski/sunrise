#!/usr/bin/env python
# -*- coding: utf-8 -*-

# from gevent.monkey import patch_all
# patch_all()

import pymongo, sys, os, random

from flask import Flask, render_template, request, jsonify, redirect, url_for, escape, json, abort
from werkzeug.contrib.fixers import ProxyFix
from tools import parse_playlist, raw_redirect

sys.path.append(os.path.abspath('../../loader'))
from client import SimpleClient

app = Flask(__name__)
app.wsgi_app = ProxyFix(app.wsgi_app)
app.config.from_pyfile('config.py')

db = pymongo.Connection()[app.config['DBNAME']]

class User(object):
	id = 1

user = User()

# backend client
bd = SimpleClient(('localhost', 4243))
bd.debug = True

@app.route('/')
def streams():
	channels = bd.stream_get_channels(owner_id=user.id, content_type=['audio/mpeg']).get('Items') or []
	for channel in channels:
		channel['playinfo'] = {
			'url': url_for('play_redirect', radio_id=channel['RadioId']),
			'radioId': channel['RadioId'],
			'bitrate': sorted(channel['Bitrate']),
			'title': channel['Title'],
		}
	return render_template('streams.html', channels=channels)

@app.route('/playrd')
def play_redirect():
	radio_id = request.args.get('radio_id', type=int)
	bitrate = request.args.get('bitrate', type=int)

	streams = bd.stream_get(owner_id=user.id, radio_id=radio_id, bitrate=bitrate, content_type=['audio/mpeg'])
	streams = streams.get('Items') or []
	if not streams:
		abort(404)

	stream = random.choice(streams)
	return raw_redirect(stream['Url'])

@app.route('/upload_playlist', methods=['POST'])
def upload_playlist():
	playlist = request.files.get('playlist')
	if playlist:
		# TODO: check mime and len
		content = playlist.stream.read()
		urls = parse_playlist(content.decode('utf-8', errors='ignore'))
		if urls:
			result = dict((url, bd.stream_save(url=url, owner_id=user.id)) for url in urls)
			print result

	return redirect(url_for('streams'))

@app.context_processor
def context():
    return {
        'need_layout': True,
    }

@app.template_filter('json')
def template_filter_json(data):
    return escape(json.dumps(data))    

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=13017, debug=True)
