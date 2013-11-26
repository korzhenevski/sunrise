#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pymongo
from flask import Flask, render_template
from datetime import datetime
app = Flask(__name__)

db = pymongo.Connection()['test']

@app.template_filter()
def format_ts(ts):
	return datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%I:%S')

@app.template_filter()
def queue_html_class(s):
    c = ''
    if s.get('task_id'):
        c = 'active'

    if s.get('record') and c:
        c = 'warning'

    if s.get('retry_ivl'):
        c = 'danger'

    return c

@app.route("/")
def hello():
    stat = db.queue.find()
    return render_template('admin.html', stat=stat)

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=13017, debug=True)
