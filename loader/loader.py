#!/usr/bin/python

import argparse
from client import Client
from zlib import crc32
from pprint import pprint as pp


def main():
	parser = argparse.ArgumentParser()
	parser.add_argument("source", type=str, help='streams file or url')

	parser.add_argument("--server_id", type=int, default=0)
	parser.add_argument("--retry_min", type=int, default=10)
	parser.add_argument("--retry_max", type=int, default=3600)
	parser.add_argument("--user_agent", type=str, default='')
	parser.add_argument("--tracker", type=str, default='de5:4242')
	parser.add_argument("--record", action="store_true")
	parser.add_argument("--duration", type=int, default=3600)
	parser.add_argument("--size", type=int, default=(128<<20), help='max record blob size (default 20MB)')

	args = parser.parse_args()
	print args

	addr = args.tracker.split(':')
	print (addr[0], int(addr[1]))
	client = Client((addr[0], int(addr[1])))

	if args.source.startswith('http'):
		urls = [args.source]
	else:
		urls = open(args.source)

	for url in urls:
		url = url.strip()
		if not url:
			continue

		print '--' * 10, url
		task = {
			'StreamId': crc32(url) & 0xFFFFFFFF,
			'StreamUrl': url,
			'ServerId': args.server_id,
			'Record': args.record,
			'LimitRecordDuration': args.duration,
			'LimitRecordSize': args.size,
			'MinRetryInterval': args.retry_min,
			'MaxRetryInterval': args.retry_max,
			'UserAgent': args.user_agent,
		}
		pp(task)
		print '--' * 10, 'put task'
		pp(client.call("Tracker.PutTask", task))


if __name__ == '__main__':
	main()
