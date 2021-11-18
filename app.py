from google.transit import gtfs_realtime_pb2
import requests
from datetime import date, datetime
import pytz
import pandas as pd
import sys


trains = {
    'A' : 'ace',
    'C' : 'ace',
    'E' : 'ace',
    'B' : 'bdfm',
    'D' : 'bdfm',
    'F' : 'bdfm',
    'M' : 'bdfm',
    'G' : 'g',
    'L' : 'l',
    'F' : 'bdfm',
    'N' : 'nqrw',
    'Q' : 'nqrw',
    'R' : 'nqrw',
    'W' : 'nqrw',
    'S' : 'si'
}
stops = pd.read_csv('stops.txt')
routes = pd.read_csv('routes.txt')
feed = gtfs_realtime_pb2.FeedMessage()
time_format = '%I:%M:%S %p'
selected_train = sys.argv[1].upper()
all_associated_trains = routes[routes['route_id'].str.contains(selected_train.upper(), na=False)]['route_id'].to_list()
headers = {'x-api-key': '1B0IaqQdwEarXZhpFkhqS8EMu5mj5s7g1kKx1591'}
if selected_train in trains.keys():
    url = "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-" + trains[selected_train]
elif selected_train in ['1', '2', '3', '4', '5', '6', '7']:
    url = "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs"
else:
    print('Try again with a proper train symbol.')
    exit()

response = requests.request("GET", url, headers=headers)
feed.ParseFromString(response.content)

stops_and_times = pd.DataFrame()
for entity in feed.entity:
    if entity.HasField('trip_update'):
        for item in entity.trip_update.stop_time_update:
            departure_time = item.departure.time

            est_time = datetime.fromtimestamp(item.departure.time, tz= pytz.timezone('America/New_York'))
            adjusted_time = datetime.fromtimestamp(item.departure.time - item.departure.delay, tz= pytz.timezone('America/New_York'))

            try:
                stop_name = stops[stops['stop_id'] == item.stop_id]['stop_name'].iloc[0]
                # stops_and_times[stop_name] = est_time.strftime(time_format)
                data = {'stop':stop_name, 
                        'time':est_time.strftime(time_format),
                        'route': entity.trip_update.trip.route_id ,
                        # 'int_time': item.departure.time,
                        # 'stop_sequence': item.stop_sequence,
                        'trip': entity.trip_update.trip.trip_id.split('..')[1],
                        'delay': item.departure.delay,
                        'adjusted_time': adjusted_time.strftime(time_format)}
                stops_and_times = stops_and_times.append(data, ignore_index=True)
            except:
                pass

stops_and_times = stops_and_times[pd.to_datetime(stops_and_times['time']) > datetime.now()]

if len(sys.argv) > 1:
    stops_and_times = stops_and_times[stops_and_times['route'].isin(all_associated_trains)]

if len(sys.argv) > 2:
    input_stop_station = sys.argv[2]
    stops_and_times.index.name = 'id'
    stops_and_times = stops_and_times[stops_and_times['stop'] == input_stop_station].sort_values(by=['trip', 'adjusted_time'])

stops_and_times.to_csv('output.csv')
print(stops_and_times)
# for entity in feed.entity:
#     print(entity.trip_update)
# print(feed.header)