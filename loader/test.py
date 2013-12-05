from client import Client
from pprint import pprint as pp
import pymongo

c = Client(('localhost', 4242))
db = pymongo.Connection()['test']

#db.ch.remove()

def hold(name, sid, tid):
	c.call('Tracker.HoldChannel', {'Name': name, 'StreamId': sid, 'TaskId': tid})

hold('AH.FM', 1984, 865685590)
hold('AH.FM', 1986, 4007759820)
hold('AH.FM', 1982, 3963036175)

pp(list(db.ch.find()))
