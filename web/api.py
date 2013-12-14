#!/usr/bin/env python
# -*- coding: utf-8 -*-

from app import app, db, Search
from flask import Flask, Response, make_response, abort, request, jsonify, url_for, redirect

# ----------- API

@app.route('/api/stream.getList')
def api_stream_list():
    return jsonify({'streams': list(db.streaminfo.find(fields=['_id', 'name']))})


@app.route('/api/stream.getDays')
def api_stream_days():
    stream_id = request.args.get('id', type=int)
    if not stream_id:
        abort(400)

    days_facet = Search().get_stream_days(stream_id)
    return jsonify({'days': sorted(term['term'] for term in days_facet['terms'])})

@app.route('/api/stream.getRecords')
def api_stream_records():
    stream_id = request.args.get('id', type=int)
    if not stream_id:
        abort(400)

    date = request.args.get('date', type=str)
    if not date:
        abort(400)

    records = []
    for result in Search().stream_records(stream_id, date=date).results:
        records.append({
            'title': result['title'],
            'duration': result['duration'],
            'ts': result['ts'],
            'url': full_url_for('play_redirect', record_id=result['record_id']),
        })

    return jsonify({'records': records})