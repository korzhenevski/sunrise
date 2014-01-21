#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pymongo
from flask import Flask, render_template, jsonify, request
from datetime import datetime
app = Flask(__name__)

db = pymongo.Connection()['test']

@app.route("/")
def hello():
    return render_template('admin.html')

@app.route("/queue.json")
def api_queue():
	results = list(db.queue.find(fields={'runtime': 0, 'opresult': 0}))
	return jsonify({
	    'success': True,
	    'data': results
	})

@app.route("/queue.json", method=['DELETE'])
def api_queue():
	print request.json()['_id']
	db.queue.
	return jsonify({
	    'success': True,
	    'data': []
	})


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=13017, debug=True)
