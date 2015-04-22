import websocket, json
from datastreams import *
from pprint import pprint


def rsvp_source():
    ws = websocket.create_connection('ws://stream.meetup.com/2/rsvps')
    while True:
        yield ws.recv()

def log(rsvp):
    with open('rsvps.json', 'a') as log:
        log.write(json.dumps(rsvp) + '\n')

DataStream(rsvp_source())\
    .filter(lambda s: 'seattle' in s.lower())\
    .map(json.loads)\
    .for_each(log)\
    .for_each(pprint)\
    .execute()


################################################################################

#def log(rsvp):
#    with open('rsvps.json', 'a') as log:
#        log.write(json.dumps(rsvp) + '\n')
#
#ws = websocket.create_connection('ws://stream.meetup.com/2/rsvps')
#while True:
#    rsvp = json.loads(ws.recv())
#    if rsvp['response'] == 'yes':
#        log(rsvp)
#        pprint(rsvp)
