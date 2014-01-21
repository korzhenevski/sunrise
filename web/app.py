#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pymongo
import flask
import time

from flask import Flask, render_template, make_response, abort, request, jsonify
from werkzeug.contrib.fixers import ProxyFix
from dictshield.document import Document
from dictshield.fields import *
from datetime import datetime, timedelta

MONTH_NAMES = (u"янв", u"фев", u"мар", u"апр", u"май", u"июн", u"июл", u"авг", u"сен", u"окт", u"ноя", u"дек")

app = Flask(__name__)
app.wsgi_app = ProxyFix(app.wsgi_app)
app.config.from_pyfile('config.py')
db = pymongo.Connection()['test2']

def get_ts():
    return int(time.time())

def full_url_for(*args, **kw):
    return app.config['BASEURL'] + flask.url_for(*args, **kw)


def safe_string(name):
    return request.args.get(name, type=unicode, default=u'')[:200].strip()


def safe_int(name):
    return abs(request.args.get(name, type=int, default=0))


def check_preload_finish(s):
    if not request.args.get('preload'):
        return
    return not s.total or not len(s.results)


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


@app.route('/play_redirect/<int:record_id>')
def play_redirect(record_id):
    record = db.records.find_one({'_id': record_id})
    if not record:
        abort(404)

    server = db.servers.find_one({'_id': record['server_id']})
    if not server:
        abort(404)

        #h = url_hash(record['path'], ip=request.remote_addr, expires=timedelta(hours=1), secret='secret')
        #return redirect(server['url'] + record['path'])


@app.route('/')
def radio_list():
    return render_template('radio_list.html', radio_list=Radio.from_list(db.radio.find()))

def is_good_title(s):
    if not s:
        return

    alpha = 0
    space = 0
    lower = 0
    upper = 0

    for c in s:
        if c.isalpha():
            alpha += 1

        if c.isspace():
            space += 1

        if c.islower():
            lower += 1

        if c.isupper():
            upper += 1

    if len(s) < 10:
        return False

    if not space or not alpha:
        return

    space_trasher = float(space) / len(s)

    if space_trasher < 0.02 or space_trasher >= 0.25:
        return False

    # upper_trasher = float(upper) / len(s)
    # lower_trasher = float(lower) / len(s)

    return True


@app.route('/radio/<int:radio_id>')
def radio_channel(radio_id):
    chan = db.radio.find_one({'channel_id': radio_id})
    if not chan:
        abort(404)

    chan = Radio(**chan)
    return render_template('chan.html', chan=chan)


def old_channel():
    pass
    #from collections import deque
    #result = {}
    #fields = {
    #'_id': False,
    #'title': True,
    ## 'record_id': True, 'ts': True
    #}
    #where = {'channel_id': radio_id, 'ts': {'$gt': get_ts() - 86400 * 5}}
    #results = []
    #last_title = None
    #
    #for item in db.air.find(where, fields=fields, sort=[('ts', -1)]).limit(100):
    #    if request.args.get('no'):
    #        results.append(item)
    #        continue
    #
    #    if last_title is not None and last_title == item['title']:
    #        continue
    #
    #    last_title = item['title']
    #
    #    if is_good_title(item['title']):
    #        results.append(item)
    #
    #return jsonify({'results': results})
    # radio = db.radio.find_one({'channel_id': radio_id})
    # if not radio:
    #     abort(404)

    # return render_template('radio.html', radio=Radio(**radio))



class BaseDocument(Document):
    _meta = {'allow_inheritance': False}

    @classmethod
    def from_list(cls, it):
        return map(lambda x: cls(**x), it)


class Audio(BaseDocument):
    title = StringField()
    duration = IntField()


class Radio(BaseDocument):
    name = StringField()
    onair = DictField()
    channel_id = LongField()

    @property
    def title(self):
        return self.onair.get('info', {}).get('name', 'Untitled')

    def get_last_audio(self, limit=None):
        it = db.air.find({'channel_id': self.channel_id, 'duration': {'$gt': 20}}, limit=limit).sort('ts', -1)
        return Audio.from_list(it)


@app.template_filter()
def humantime(dt):
    if isinstance(dt, int):
        dt = datetime.fromtimestamp(dt)

    delta = datetime.now() - dt
    days = abs(delta.days)
    years = days // 365
    daymap = {0: u'сегодня', 1: u'вчера', 2: u'позавчера'}
    month = MONTH_NAMES[dt.month - 1]

    if years == 0:
        if days == 0:
            day = u''
        elif days <= 2:
            day = daymap.get(days)
        else:
            day = u'%d %s' % (dt.day, month)
    else:
        day = u'%d %s %d' % (dt.day, month, dt.year)

    return unicode(day + u' в ' + dt.strftime(u"%H:%M"))


@app.template_filter()
def format_ts(ts, fmt=None):
    if not fmt:
        fmt = '%Y-%m-%d %H:%I:%S'
        return datetime.fromtimestamp(ts).strftime(fmt)


@app.template_filter()
def toduration(dur):
    dur = int(dur)
    if dur > 0:
        res = unicode(timedelta(seconds=dur))
        if dur <= 3600:
            return res[2:]
        return res
    return '00:00'


# import template_filters

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=13017, debug=True)
