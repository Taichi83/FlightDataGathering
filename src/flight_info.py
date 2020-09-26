from traffic.data import opensky
import os
import datetime
import pytz
from copy import deepcopy
from tqdm import tqdm
from dateutil.relativedelta import *

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
                                       onground=onground, min_ft=min_ft,
                                       time_interval=datetime.timedelta(minutes=1),
                                       start_str=start_str, end_str=end_str)
        list_out.append(df_temp)
    except AttributeError:
        print('nodata {0}-{1}'.format(start_str, end_str))
    return pd.concat(list_out)


class HistoricalLocationsData(object):
    def __init__(self,
                 file_batch_unit='daily',
                 time_interval=datetime.timedelta(minutes=1),
                 on_ground=False,
                 min_ft=33000):
        self.file_batch_unit = file_batch_unit
        self.time_interval = time_interval
        self.on_ground = on_ground
        self.min_ft = min_ft

    def _remove_row_flight_df(self, df, start_str=None, end_str=None):
        sr_bool = df['altitude'] >= self.min_ft
        if self.on_ground is not None:
            sr_on_ground = df['onground'] == self.on_ground
            sr_bool = sr_bool & sr_on_ground
        if self.time_interval is not None:
            start_datetime = datetime.datetime.strptime(start_str + ' 1', '%Y-%m-%d %H:%M %S').replace(tzinfo=pytz.UTC)
            end_datetime = datetime.datetime.strptime(end_str + ' 1', '%Y-%m-%d %H:%M %S').replace(
                tzinfo=pytz.UTC) - datetime.timedelta(minutes=1)
            list_datetime = []
            while start_datetime <= end_datetime:
                list_datetime.append(start_datetime)
                start_datetime = start_datetime + self.time_interval
            sr_time = df['timestamp'].isin(list_datetime)
            sr_bool = sr_bool & sr_time
        df = df[sr_bool].sort_values(by=['timestamp'])
        return df

    def get_df_one_unit(self, target_date, callsign=None, icao24=None, departure_airport=None, arrival_airport=None,
                        calc_interval_datetime=datetime.timedelta(hours=1), save_local=False, dir_save=None,
                        pickle=True,
                        tqdm_count=True):
        if self.file_batch_unit == 'daily':
            filename_head = target_date.strftime('%Y%m%d') + '_' + 'D'
            start_datetime = datetime.datetime.combine(target_date, datetime.datetime.min.time())
            stop_datetime = start_datetime + datetime.timedelta(days=1)

            # start_str = start_datetime.strftime('%Y-%m-%d %H:%M')
            # stop_str = stop_datetime.strftime('%Y-%m-%d %H:%M')

            # start_str = target_date.strftime('%Y-%m-%d') + ' 00:00'
            # stop_str = (target_date + datetime.timedelta(days=1)).strftime('%Y-%m-%d') + ' 00:00'


        elif self.file_batch_unit == 'monthly':
            filename_head = target_date.strftime('%Y%m') + '_' + 'M'
            target_date = datetime.date(year=target_date.year, month=target_date.month, day=1)
            start_datetime = datetime.datetime.combine(target_date, datetime.datetime.min.time())
            stop_datetime = datetime.datetime.combine((target_date + relativedelta(months=+1)),
                                                      datetime.datetime.min.time())

            # start_str = target_date.strftime('%Y-%m') + '-01 00:00'
            # stop_str = (target_date + relativedelta(months=+1)).strftime('%Y-%m') + '-01 00:00'

        else:
            print('check arg file_batch_unit')
            return

        if callsign is not None:
            filename_head = filename_head + '_' + 'L'
        else:
            filename_head = filename_head + '_' + 'A'

        if icao24 is not None:
            filename_head = filename_head + '_' + 'L'
        else:
            filename_head = filename_head + '_' + 'A'

        if departure_airport is not None:
            filename_head = filename_head + '_' + 'dpt-' + departure_airport
        else:
            filename_head = filename_head + '_' + 'dpt-all'

        if arrival_airport is not None:
            filename_head = filename_head + '_' + 'arr-' + arrival_airport
        else:
            filename_head = filename_head + '_' + 'arr-all'

        if calc_interval_datetime is None:
            start_str = start_datetime.strftime('%Y-%m-%d %H:%M')
            stop_str = stop_datetime.strftime('%Y-%m-%d %H:%M')
            flight = opensky.history(
                start=start_str,
                stop=stop_str,
                callsign=callsign,
                icao24=icao24,
                departure_airport=departure_airport,
                arrival_airport=arrival_airport,
                cached=False
            )
            try:
                df_out = flight.data
                df_out = self._remove_row_flight_df(df_out, start_str=start_str, end_str=stop_str)
            except AttributeError:
                print('nodata {0}-{1}'.format(start_str, stop_str))
                return None
        else:
            start_datetime_temp = deepcopy(start_datetime)
            stop_datetime_temp = start_datetime_temp + calc_interval_datetime
            list_df_out = []
            if tqdm_count:
                pbar = tqdm(total=(stop_datetime - start_datetime) // calc_interval_datetime)
            while stop_datetime_temp <= stop_datetime:
                start_str = start_datetime_temp.strftime('%Y-%m-%d %H:%M')
                stop_str = stop_datetime_temp.strftime('%Y-%m-%d %H:%M')
                if tqdm_count:
                    pbar.update(1)
                    pbar.set_description(start_str)
                flight = opensky.history(
                    start=start_str,
                    stop=stop_str,
                    callsign=callsign,
                    icao24=icao24,
                    departure_airport=departure_airport,
                    arrival_airport=arrival_airport,
                    cached=False
                )
                try:
                    df_temp = flight.data
                    df_temp = self._remove_row_flight_df(df_temp, start_str=start_str, end_str=stop_str)
                    list_df_out.append(df_temp)
                except AttributeError:
                    print('nodata {0}-{1}'.format(start_str, stop_str))
                start_datetime_temp = start_datetime_temp + calc_interval_datetime
                stop_datetime_temp = start_datetime_temp + calc_interval_datetime

            if tqdm_count:
                pbar.close()

            start_str = start_datetime_temp.strftime('%Y-%m-%d %H:%M')
            stop_str = stop_datetime.strftime('%Y-%m-%d %H:%M')
            flight = opensky.history(
                start=start_str,
                stop=stop_str,
                callsign=callsign,
                icao24=icao24,
                departure_airport=departure_airport,
                arrival_airport=arrival_airport
            )
            try:
                df_temp = flight.data
                df_temp = self._remove_row_flight_df(df_temp, start_str=start_str, end_str=stop_str)
                list_df_out.append(df_temp)
            except AttributeError:
                print('nodata {0}-{1}'.format(start_str, stop_str))
            df_out = pd.concat(list_df_out)

        if save_local:
            if not os.path.exists(dir_save):
                os.makedirs(dir_save)

            path_dest = os.path.join(dir_save, filename_head)
            if pickle:
                path_dest = path_dest + '.pkl'
                df_out.to_pickle(path=path_dest)
            else:
                path_dest = path_dest + '.csv'
                df_out.to_csv(path_dest, index=False)

            return df_out, path_dest

        return df_out, None

    def get_df_time_range(self, start_date, stop_date, callsign=None, icao24=None, departure_airport=None,
                          arrival_airport=None,
                          calc_interval_datetime=datetime.timedelta(hours=1), save_local=False, dir_save=None,
                          pickle=True):
        if self.file_batch_unit == 'daily':
            list_path = []
            target_date = deepcopy(start_date)
            pbar = tqdm(total=(stop_date - start_date) // datetime.timedelta(days=1))
            while target_date + datetime.timedelta(days=1) <= stop_date:
                pbar.update(1)
                pbar.set_description(target_date.strftime('%Y-%m-%d'))
                df_out, path_dest = self.get_df_one_unit(target_date=target_date,
                                                         callsign=callsign,
                                                         icao24=icao24,
                                                         departure_airport=departure_airport,
                                                         arrival_airport=arrival_airport,
                                                         calc_interval_datetime=calc_interval_datetime,
                                                         save_local=save_local,
                                                         dir_save=dir_save,
                                                         pickle=pickle,
                                                         tqdm_count=False
                                                         )
                list_path.append(path_dest)
                target_date = target_date + datetime.timedelta(days=1)

            pbar.close()
        elif self.file_batch_unit == 'monthly':
            list_path = []
            target_date = datetime.date(year=start_date.year, month=start_date.month, day=1)

            pbar = tqdm(total=(stop_date.year - 2020) * 12 + stop_date.month - (
                        target_date.year - 2020) * 12 + target_date.month + 1)
            while target_date + relativedelta(months=+1) <= stop_date:
                pbar.update(1)
                pbar.set_description(target_date.strftime('%Y-%m-%d'))
                df_out, path_dest = self.get_df_one_unit(target_date=target_date,
                                                         callsign=callsign,
                                                         icao24=icao24,
                                                         departure_airport=departure_airport,
                                                         arrival_airport=arrival_airport,
                                                         calc_interval_datetime=calc_interval_datetime,
                                                         save_local=save_local,
                                                         dir_save=dir_save,
                                                         pickle=pickle,
                                                         tqdm_count=False
                                                         )
                list_path.append(path_dest)
                target_date = target_date + relativedelta(months=+1)
            pbar.close()
        else:
            print('check arg file_batch_unit')
            return

        return list_path


def test_1():
    airport_depart_name_key = 'Fukuoka'
    airport_arrival_name_key = 'Haneda'
    start_datetime = datetime.datetime(year=2018, month=11, day=14, hour=8, minute=0, tzinfo=pytz.utc)
    end_datetime = datetime.datetime(year=2018, month=11, day=15, hour=8, minute=0, tzinfo=pytz.utc)
    interval_datetime = datetime.timedelta(hours=1)
    onground = False
    min_ft = 33000
    time_interval = datetime.timedelta(minutes=1)

    df_airport = pd.read_csv(PATH_AIRPORT_INFO)

    airport_depart_ident = df_airport[df_airport['name'].str.contains(airport_depart_name_key)]['ident'].to_list()
    airport_arrival_ident = df_airport[df_airport['name'].str.contains(airport_arrival_name_key)]['ident'].to_list()
    print(airport_depart_ident, airport_arrival_ident)

    df = get_history_data(start_datetime, end_datetime,
                          interval_datetime=interval_datetime,
                          callsign=None,
                          icao24=None,
                          departure_airport=airport_depart_ident[0],
                          arrival_airport=airport_arrival_ident[0],
                          onground=onground, min_ft=min_ft,
                          time_interval=time_interval
                          )

    print(df)


def test_2():
    airport_depart_name_key = 'Fukuoka'
    airport_arrival_name_key = 'Haneda'

    target_date = datetime.date(year=2018, month=11, day=14)
    callsign = None
    icao24 = None
    calc_interval_datetime = datetime.timedelta(hours=1)

    save_local = True
    dir_save = 'data/output'
    pickle = False

    file_batch_unit = 'daily'
    time_interval = datetime.timedelta(minutes=1)

    df_airport = pd.read_csv(PATH_AIRPORT_INFO)
    airport_depart_ident = df_airport[df_airport['name'].str.contains(airport_depart_name_key)]['ident'].to_list()
    airport_arrival_ident = df_airport[df_airport['name'].str.contains(airport_arrival_name_key)]['ident'].to_list()
    print(airport_depart_ident, airport_arrival_ident)
    airport_depart_ident = airport_depart_ident[0]
    airport_arrival_ident = airport_arrival_ident[0]

    historical_locations_data = HistoricalLocationsData(file_batch_unit=file_batch_unit,
                                                        time_interval=time_interval
                                                        )
    df = historical_locations_data.get_df_one_unit(target_date=target_date,
                                                   callsign=callsign,
                                                   icao24=icao24,
                                                   departure_airport=airport_depart_ident,
                                                   arrival_airport=airport_arrival_ident,
                                                   calc_interval_datetime=calc_interval_datetime,
                                                   save_local=save_local,
                                                   dir_save=dir_save,
                                                   pickle=pickle
                                                   )


def test_3():
    # todo: airport should not be decided for searching because the time range
    airport_depart_name_key = 'Fukuoka'
    airport_arrival_name_key = 'Haneda'

    start_date = datetime.date(year=2018, month=11, day=14)
    stop_date = datetime.date(year=2018, month=11, day=16)
    callsign = None
    icao24 = None
    calc_interval_datetime = datetime.timedelta(hours=1)

    save_local = True
    dir_save = 'data/output'
    pickle = False

    file_batch_unit = 'daily'
    time_interval = datetime.timedelta(minutes=1)

    df_airport = pd.read_csv(PATH_AIRPORT_INFO)
    airport_depart_ident = df_airport[df_airport['name'].str.contains(airport_depart_name_key)]['ident'].to_list()
    airport_arrival_ident = df_airport[df_airport['name'].str.contains(airport_arrival_name_key)]['ident'].to_list()
    print(airport_depart_ident, airport_arrival_ident)
    airport_depart_ident = airport_depart_ident[0]
    airport_arrival_ident = airport_arrival_ident[0]

    historical_locations_data = HistoricalLocationsData(file_batch_unit=file_batch_unit,
                                                        time_interval=time_interval
                                                        )
    list_path = historical_locations_data.get_df_time_range(start_date=start_date,
                                                            stop_date=stop_date,
                                                            callsign=callsign,
                                                            icao24=icao24,
                                                            departure_airport=airport_depart_ident,
                                                            arrival_airport=airport_arrival_ident,
                                                            calc_interval_datetime=calc_interval_datetime,
                                                            save_local=save_local,
                                                            dir_save=dir_save,
                                                            pickle=pickle
                                                            )
    print(list_path)

    return


def main():
    # test_1()
    # test_2()
    test_3()

if __name__ == "__main__":
    main()
