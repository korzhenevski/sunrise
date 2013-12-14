#!/usr/bin/python
# -*- coding: utf-8 -*-

#from gevent.monkey import patch_all
#patch_all()

import requests
from pymongo.connection import MongoClient
import re
import string
import time
import argparse

from datetime import datetime
from pprint import pprint as pp
from elasticsearch import Elasticsearch
from stathat import StatHat


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--last_id", type=int, default=1)
    parser.add_argument("--index", type=str, default='sunrise3')
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--cached", action="store_true")
    parser.add_argument("--stathat", action="store_true")

    args = parser.parse_args()
    print args

    db = MongoClient()['test']
    es = Elasticsearch()
    stats = StatHat('hq08Ng2ujA8o3VPe')

    lastfm_url = "http://ws.audioscrobbler.com/2.0/?api_key=048cd62229f507d6d577393a6d7ac972&format=json"
    factory = Factory(db, lastfm_url)
    factory.cached = args.cached

    last_id = args.last_id

    while True:
        where = {'_id': {'$gt': last_id}, 'end_ts': {'$gt': 0}}
        #where = {'_id': {'$gt': last_id}}
        if not args.force:
            where['pub_ts'] = 0

        print where

        oid = last_id
        for air in db.air.find(where).sort('ts').limit(100):
            oid = air['_id']
            audio = factory.build_audio_from_air(air)
            es.index(index=args.index, doc_type='audio', id=oid, body=audio)
            if not args.force:
                db.air.update({'_id': oid}, {'$set': {'pub_ts': int(time.time())}})
            print '---' * 10, oid
            pp(audio)

            if args.stathat:
                stats.count('index.audio', 1)
                if audio.get('is_track'):
                    stats.count('index.track', 1)

        if oid == last_id:
            if args.force:
                continue
            else:
                print 'wait for new tracks...'
                time.sleep(10)
        else:
            last_id = oid


# TODO: optimize RE init
def strip_control_chars(s):
    """
    >>> strip_control_chars("\x01\x02")
    ''
    """
    if not s:
        return s
        # unicode invalid characters
    RE_XML_ILLEGAL = u'([\u0000-\u0008\u000b-\u000c\u000e-\u001f\ufffe-\uffff])|'
    RE_XML_ILLEGAL += u'([%s-%s][^%s-%s])|([^%s-%s][%s-%s])|([%s-%s]$)|(^[%s-%s])' % (
        unichr(0xd800), unichr(0xdbff), unichr(0xdc00), unichr(0xdfff), unichr(0xd800), unichr(0xdbff), unichr(0xdc00),
        unichr(0xdfff), unichr(0xd800), unichr(0xdbff), unichr(0xdc00), unichr(0xdfff),)
    s = re.sub(RE_XML_ILLEGAL, "", s)
    s = re.sub(r"[\x01-\x1F\x7F]", "", s)
    return s


# TODO: add tests
def normalize_track_title(s):
    if not s:
        return s
    s = strip_control_chars(s)
    s = re.sub(r'[-]{2,}', '-', s)
    s = s.replace('_-_', ' - ')
    s = re.sub(r'\s{2,}', ' ', s, flags=re.U)
    s = s.strip()
    return s.encode("utf8")


def split_title(title):
    """
    >>> [split_title(x) for x in ('Artist - Track', ' Artist-Long  -  Track ', '- Testing -', 'string')]
    [['Artist', 'Track'], ['Artist-Long', 'Track'], False, False]
    """
    if '-' not in title:
        return False
        # strip lead and tailing whitespace and dashes
    title = title.strip(string.whitespace + '-')
    # split by track separator
    splitted = title.split(' - ' if ' - ' in title else '-', 1)
    if len(splitted) == 1:
        return False
    return map(string.strip, splitted)


#def remove_accents(input_str):
#    nkfd_form = unicodedata.normalize('NFKD', unicode(input_str))
#    return u"".join([c for c in nkfd_form if not unicodedata.combining(c)])

class LastfmError(RuntimeError):
    def __init__(self, code, message):
        self.code = code
        self.message = message

    def __str__(self):
        return '%s (%d)' % (self.message, self.code)


def retry(exceptions=Exception, tries=100, delay=1, backoff=2):
    def deco_retry(f):
        def f_retry(*args, **kwargs):
            mtries, mdelay = tries, delay
            while mtries > 0:
                try:
                    return f(*args, **kwargs)
                except exceptions, e:
                    print "%s, Retrying in %d seconds..." % (str(e), mdelay)
                    time.sleep(mdelay)
                    mtries -= 1
                    mdelay *= backoff
                    lastException = e
            raise lastException

        return f_retry

    return deco_retry


@retry()
def safe_lastfm_get(url, params):
    print params
    resp = requests.get(url, params=params)
    resp.raise_for_status()

    data = resp.json()
    if not data:
        raise RuntimeError('No JSON data')

    if 'error' in data:
        print data.get('error'), data.get('message')
        # raise LastfmError(data.get('error'), data.get('message'))
        return

    return data.get('track')


class Factory(object):
    def __init__(self, db, lastfm_url):
        self.db = db
        self.lastfm_url = lastfm_url
        self.cached = False

    def get_track(self, title):
        sp = split_title(title)
        if not sp:
            return False

        artist, name = sp

        track = {
            'artist': artist,
            'name': name,
            #'tags': [],
            'is_track': False,
        }

        tags = self.lookup_lastfm_tags(track['artist'].lower(), track['name'].lower())
        if tags:
            track['tags'] = tags
            track['is_track'] = True

        #try:
        #    info = self.lastfm_lookup(track['artist'].lower(), track['name'].lower(), cached=self.cached)
        #    if info:
        #        track['tags'] = self.get_lastfm_tags(info)
        #        track['is_track'] = True
        #except RuntimeError as exc:
        #    print exc

        return track

    def lookup_lastfm_tags(self, artist, name):
        cache_info = self.db.lastfm.find_one({'artist': artist, 'name': name})
        if cache_info and cache_info.get('info'):
            return self.get_lastfm_tags(cache_info['info'])
        return []

    def lastfm_lookup(self, artist, name, cached=False):
        cache_info = self.db.lastfm.find_one({'artist': artist, 'name': name})
        if cached:
            return (cache_info or {}).get('info')

        if not cache_info:
            cache_info = {'info': self.get_lastfm_info(artist, name), 'ts': datetime.now()}
            self.db.lastfm.update({'artist': artist, 'name': name}, {'$setOnInsert': cache_info}, upsert=True)

        return cache_info['info']

    def get_lastfm_info(self, artist, name):
        params = dict(method='track.getInfo', autocorrect=1, artist=artist, track=name)
        return safe_lastfm_get(self.lastfm_url, params)

    def get_lastfm_tags(self, info):
        toptags = info.get('toptags', {})
        if type(toptags) is not dict:
            return []
        return [tag['name'] for tag in toptags.get('tag', []) if type(tag) is dict]

    def build_audio_from_air(self, air):
        print air
        audio = {
            'id': air['_id'],
            'ts': air['ts'],
            #'ts': datetime.fromtimestamp(air['ts']),
            'date': datetime.fromtimestamp(air['ts']).strftime('%Y%m%d'),
            'duration': air['duration'],
            'stream_id': air['stream_id'],
        }

        if air['rid']:
            audio.update({'record_id': air['rid'], 'record_size': 0})
            rec = self.db.records.find_one({'_id': air['rid']}, fields={'size': 1})
            if rec:
                audio['record_size'] = rec['size']

        title = normalize_track_title(air['title'])
        if title:
            audio['title'] = title
            info = self.get_track(title)
            if info:
                audio.update(info)
        else:
            # для записей без метаинформации
            audio['title'] = datetime.fromtimestamp(air['ts']).strftime('%H:%M @ %Y-%m-%d')

        return audio


if __name__ == "__main__":
    main()
