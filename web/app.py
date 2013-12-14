#!/usr/bin/env python
# -*- coding: utf-8 -*-

import re
import pymongo
import json
import flask
import string
import hashlib
import base64
import urllib

from flask import Flask, Response, render_template, make_response, abort, request, jsonify, url_for, redirect
from datetime import datetime, timedelta, date
from pprint import pprint as pp
from werkzeug.contrib.fixers import ProxyFix

app = Flask(__name__)
app.wsgi_app = ProxyFix(app.wsgi_app)
app.config.from_pyfile('config.py')
db = pymongo.Connection()['test']

from search import Search


def full_url_for(ep, **kw):
    return app.config['BASEURL'] + flask.url_for(ep, **kw)

def safe_string(name):
    return request.args.get(name, type=unicode, default=u'')[:200].strip()

def safe_int(name):
    return abs(request.args.get(name, type=int, default=0))

def check_preload_finish(s):
    if not request.args.get('preload'):
        return
    return not s.total or not len(s.results)


class Audio(dict):
    @classmethod
    def from_es(cls, hit_source):
        obj = cls(hit_source)
        return obj

    @property
    def stream_q(self):
        return 'stream_id:%s' % self.get('stream_id')

    @property
    def play_url(self):
        return url_for('play_redirect', record_id=self['record_id'])

    @property
    def full_url(self):
        return app.config.BASEURL + self.play_url

    @property
    def dt(self):
        return datetime.strptime(self['ts'], "%Y-%m-%dT%H:%M:%S")

    @property
    def dt_ts(self):
        return int(self.dt.strftime("%s"))


@app.context_processor
def context_mind():
    return {
        'layout': ('X-PJAX' not in request.headers),
        'full_url_for': full_url_for,
    }


@app.route('/opensearch.xml')
def opensearch():
    response = make_response(render_template('opensearch.xml'))
    response.headers['Content-Type'] = 'text/xml'
    return response


@app.route('/artists')
def artist_list():
    artists = Search().get_artists()
    return jsonify({'artists': artists})


@app.route('/streams')
def stream_list():
    """
        All site streams
    """
    streams = list(db.streaminfo.find())
    for info in streams:
        info['letter'] = info['name'][0].upper()
    return render_template('stream_list.html', streams=streams)


# @app.route('/')
# def latest():
#     """
#         Query search
#     """
#     # s = Search(page=safe_int('page')).latest()

#     if check_preload_finish(s):
#         return ''

#     return render_template('search.html', search=s)


@app.route('/search')
def search():
    """
        Query search
    """
    s = Search(page=safe_int('page'))
    q = re.escape(safe_string('q'))
    if q:
        s.query(q)

    if check_preload_finish(s):
        return ''

    return render_template('search.html', search=s, search_query=q)


@app.route('/stream/<int:stream_id>')
def stream(stream_id):
    """
        Stream page
    """
    stream_info = db.streams.find_one({'_id': stream_id})
    if not stream_info:
        abort(404)

    stream_info.update(db.streaminfo.find_one({'_id': stream_id}) or {})

    s = Search(page=safe_int('page'), size=20).stream_records(stream_id)
    if check_preload_finish(s):
        return ''

    rss_url = full_url_for('stream_rss', stream_id=stream_id)
    return render_template('stream.html', search=s, rss_url=rss_url, stream_info=stream_info,
                           itunes_url=rss_url.replace('http://', 'itpc://'))


@app.route('/stream/<int:stream_id>/feed')
@app.route('/stream/<int:stream_id>/rss')
def stream_rss(stream_id):
    """
        Stream RSS & iTunes Feed
    """
    s = Search(page=safe_int('page')).stream_records(stream_id)
    return Response(render_template('rss.xml', search_result=s), mimetype='text/xml')


# TODO: move record to air ?
def url_hash(path, ip, expires, secret):
    expires = int((datetime.now() + expires).strftime("%s"))
    s = '%d%s%s %s' % (expires, path, ip, secret)
    print s
    h = base64.urlsafe_b64encode(hashlib.md5(s).digest())
    return path + '?' + urllib.urlencode({'md5': h, 'expires': expires})


@app.route('/play_redirect/<int:record_id>.mp3')
def play_redirect(record_id):
    record = db.records.find_one({'_id': record_id})
    if not record:
        abort(404)

    server = db.servers.find_one({'_id': record['server_id']})
    if not server:
        abort(503)

    h = url_hash(record['path'], ip=request.remote_addr, expires=timedelta(hours=1), secret='secret')
    return redirect(server['url'] + record['path'])


import api
import template_filters

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=13017, debug=True)
