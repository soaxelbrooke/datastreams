import requests
import json
from pprint import pprint
from datetime import datetime
from collections import defaultdict
from datastreams import *
from dictstreams import *

apptoken = open('/home/stuart/Projects/datastream/res/spd_socrata.key').read().strip()

api_query = 'https://data.seattle.gov/resource/3k2p-39jp.json?$limit={}&$offset={}&$$app_token=' + apptoken
batchsize = 1000

def fetch_police_stats():
    offset = 0
    results = json.loads(requests.get(api_query.format(batchsize, offset)).content)
    for result in results:
        yield result
    while len(results) > 999:
        offset += batchsize
        results = json.loads(requests.get(api_query.format(batchsize, offset)).content)
        for result in results:
            yield result

spddata = DictStream(fetch_police_stats())
data = spddata.take(5000).collect()

class Location(object):
    def __init__(self, general_offense_number, latitude, longitude):
        self.general_offense_number = general_offense_number
        self.latitude = latitude
        self.longitude = longitude
    def __repr__(self):
        return '<Location {}>'.format(self.__dict__)

class Event(object):
    def __init__(self, general_offense_number, clearance_group, clearance_description, clearance_time):
        self.general_offense_number = general_offense_number
        self.clearance_group = clearance_group
        self.clearance_description = clearance_description
        self.clearance_time = datetime.strptime(clearance_time, "%Y-%m-%dT%H:%M:%S")
    def __repr__(self):
        return '<Event {}>'.format(self.__dict__)

locations = DataSet(data.map(lambda row: Location(row['general_offense_number'], row['latitude'], row['longitude'])))
events = DataSet(data.map(lambda row: Event(row['general_offense_number'],
                                            row['event_clearance_group'],
                                            row['event_clearance_description'],
                                            row['event_clearance_date'])))

eventlocs = events.join(locations, 'general_offense_number')

eventlocs.filter(lambda eventloc: 'disturbance' in eventloc.event_clearance_group.lower()).take(5)
