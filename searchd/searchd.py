#!/usr/bin/env python
# -*- coding: utf-8 -*-

import oursql
from flask import Flask, jsonify, request, abort, make_response

app = Flask(__name__)
# app.config.from_pyfile('config.py')

db = oursql.connect(host='127.0.0.1', port=9306)

@app.errorhandler(oursql.ProgrammingError)
def handle_invalid_usage(error):
    response = jsonify({'error': '%s' % error, 'result': []})
    response.status_code = 500
    return response

@app.route('/query')
def query():
	q = request.args.get('q', type=unicode)
	if not q:
		abort(400)

	c = db.cursor(oursql.DictCursor)
	c.execute(q, plain_query=True)
	result = c.fetchall()
	return jsonify({'result': result})

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=8082, debug=True)	