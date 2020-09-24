from traffic.data import opensky
import os
import datetime
import pytz
from copy import deepcopy
from tqdm import tqdm

import pandas as pd

USER = os.getenv('USERNAME')
PASSWORD = os.getenv('PASSWORD')
PATH_AIRPORT_INFO = 'data/airports.csv'

opensky.username = USER
opensky.password = PASSWORD


def remove_row_flight_df(df, onground=False, min_ft=33000,
                         time_interval=datetime.timedelta(minutes=1),
                         start_str=None,
                         end_str=None):
    sr_bool = df['altitude'] >= min_ft
    if onground is not None:
        sr_onground = df['onground'] == onground
        sr_bool = sr_bool & sr_onground
    if time_interval is not None:
        start_datetime = datetime.datetime.strptime(start_str + ' 1', '%Y-%m-%d %H:%M %S').replace(tzinfo=pytz.UTC)
        end_datetime = datetime.datetime.strptime(end_str + ' 1', '%Y-%m-%d %H:%M %S').replace(
            tzinfo=pytz.UTC) - datetime.timedelta(minutes=1)
        list_datetime = []
        while start_datetime <= end_datetime:
            list_datetime.append(start_datetime)
            start_datetime = start_datetime + time_interval
        sr_time = df['timestamp'].isin(list_datetime)
        sr_bool = sr_bool & sr_time
    df = df[sr_bool].sort_values(by=['timestamp'])
    return df


def get_history_data(start_datetime, end_datetime, interval_datetime=datetime.timedelta(hours=1), callsign=None,
                     icao24=None, departure_airport=None, arrival_airport=None, onground=False, min_ft=33000,
                     time_interval=datetime.timedelta(minutes=1)):
    start_datetime_temp = deepcopy(start_datetime)
    end_datetime_temp = start_datetime_temp + interval_datetime
    list_out = []
    pbar = tqdm(total=(end_datetime - start_datetime) // interval_datetime)
    while end_datetime_temp <= end_datetime:
        pbar.update(1)
        start_str = start_datetime_temp.strftime('%Y-%m-%d %H:%M')
        end_str = end_datetime_temp.strftime('%Y-%m-%d %H:%M')
        flight = opensky.history(
            start=start_str,
            stop=end_str,
            callsign=callsign,
            icao24=icao24,
            departure_airport=departure_airport,
            arrival_airport=arrival_airport
        )
        try:
            df_temp = flight.data
            df_temp = remove_row_flight_df(df_temp,
                                           onground=onground, min_ft=min_ft,
                                           time_interval=time_interval,
                                           start_str=start_str, end_str=end_str)

            list_out.append(df_temp)
        except AttributeError:
            print('nodata {0}-{1}'.format(start_str, end_str))

        start_datetime_temp = start_datetime_temp + interval_datetime
        end_datetime_temp = start_datetime_temp + interval_datetime
    pbar.close()
    start_str = start_datetime_temp.strftime('%Y-%m-%d %H:%M')
    end_str = end_datetime.strftime('%Y-%m-%d %H:%M')
    flight = opensky.history(
        start=start_str,
        stop=end_str,
        callsign=callsign,
        icao24=icao24,
        departure_airport=departure_airport,
        arrival_airport=arrival_airport
    )
    try:
        df_temp = flight.data
        df_temp = remove_row_flight_df(df_temp,
                                       onground=False, min_ft=33000,
                                       time_interval=datetime.timedelta(minutes=1),
                                       start_str=start_str, end_str=end_str)
        list_out.append(df_temp)
    except AttributeError:
        print('nodata {0}-{1}'.format(start_str, end_str))
    return pd.concat(list_out)


def main():
    airport_depart_name_key = 'Fukuoka'
    airport_arrival_name_key = 'Haneda'
    start_datetime = datetime.datetime(year=2018, month=11, day=14, hour=8, minute=0, tzinfo=pytz.utc)
    end_datetime = datetime.datetime(year=2018, month=11, day=15, hour=8, minute=0, tzinfo=pytz.utc)
    onground = False
    min_ft = 33000
    time_interval = datetime.timedelta(minutes=1)

    df_airport = pd.read_csv(PATH_AIRPORT_INFO)

    airport_depart_ident = df_airport[df_airport['name'].str.contains(airport_depart_name_key)]['ident'].to_list()
    airport_arrival_ident = df_airport[df_airport['name'].str.contains(airport_arrival_name_key)]['ident'].to_list()
    print(airport_depart_ident, airport_arrival_ident)

    df = get_history_data(start_datetime, end_datetime,
                          interval_datetime=datetime.timedelta(hours=1),
                          callsign=None,
                          icao24=None,
                          departure_airport=airport_depart_ident[0],
                          arrival_airport=airport_arrival_ident[0],
                          onground=onground, min_ft=min_ft,
                          time_interval=time_interval
                          )

    print(df)


if __name__ == "__main__":
    main()
