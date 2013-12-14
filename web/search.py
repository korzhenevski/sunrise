#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pprint import pprint as pp
from elasticsearch import Elasticsearch

es = Elasticsearch()

class Search(object):
    def __init__(self, size=100, page=1):
        self.resp = None

        self.total = 0
        self.results = []
        self.facets = {}

        self.size = size
        self.page = page

    def query(self, q):
        self._request({
            'query': {"query_string": {"query": q}},
        })

        return self

    def get_artists(self):
        self._request({
            'facets': {
                'artists': {'terms': {'field': 'artist'}}
            }
        })
        return self.facets['artists']

    def get_stream_days(self, stream_id):
        self._request({
            'facets': {
                'days': {
                    'terms': {'field': 'date'},
                    'facet_filter': {
                        'term': {'stream_id': stream_id}
                    }
                }
            }
        })

        return self.facets['days']

    def stream_records(self, stream_id, date=None):
        terms = {
            'stream_id': stream_id,
        }

        if date:
            terms['date'] = date

        self._request({
            'filter': self._get_terms_filter(terms),
            'sort': [{"ts": {"order": "desc"}}],
            'facets': {
                'days': {
                    'terms': {'field': 'date'},
                    'facet_filter': {
                        'term': {'stream_id': stream_id}
                    }
                }
            }            
        })

        return self

    def _get_terms_filter(self, terms):
        terms = [{'term': {k: v}} for k, v in terms.iteritems()]
        if len(terms) == 1:
            return terms[0]

        return {'bool': {'must': terms}}

    def latest(self):
        self._request({
            'filter': {'term': {'is_track': 'true'}},
            'sort': [{"ts": {"order": "desc"}}],
        })

        return self

    def _request(self, search_params):
    	from app import app, Audio

        search_params.update({
            'size': self.size,
            'from': abs(self.size * self.page),
        })

        pp(search_params)
        self.resp = es.search(index=app.config['INDEX'], body=search_params)
        hits = self.resp.get('hits')

        stream_ids = set(filter(None, [hit['_source'].get('stream_id') for hit in hits['hits']]))
        stream_info = dict(
            (info['_id'], info['name']) for info in db.streaminfo.find({'_id': {'$in': list(stream_ids)}}))

        results = []
        for hit in hits['hits']:
            audio = Audio.from_es(hit['_source'])
            audio['stream_name'] = stream_info.get(audio.get('stream_id'), '')
            results.append(audio)

        self.total = hits['total']
        self.results = results
        self.facets = self.resp.get('facets', {})

    def to_dict(self):
        return {
            'total': self.total,
            'results': self.results,
            'facets': self.facets,
        }