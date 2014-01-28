#!/usr/bin/env python

import sys
import os
sys.path.append(os.path.abspath('../loader'))

from client import SimpleClient
from pprint import pprint as pp

cl = SimpleClient(('localhost', 4243))
cl.debug = True
cl.stream_get_channels(owner_id=1)
#cl.radio_get(id=1)
#cl.radio_add(title='Radio Test', owner_id=1)
# cl.radio_get(owner_id=1)
# cl.radio_edit(id=40001, owner_id=1, title='Fest')
# cl.radio_get(owner_id=1)
#cl.radio_edit(id=)
