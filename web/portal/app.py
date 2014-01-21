#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pymongo

from flask import Flask, render_template
from werkzeug.contrib.fixers import ProxyFix

app = Flask(__name__)
app.wsgi_app = ProxyFix(app.wsgi_app)
app.config.from_pyfile('config.py')

db = pymongo.Connection()[app.config['DBNAME']]


@app.route('/test')
def test():
    return render_template('test.html')


@app.route('/')
def index():
    return render_template('layout.html')

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=13017, debug=True)
