import requests
import time
import json
import random
import datetime
import pytz
import os
from copy import deepcopy
from tqdm import tqdm
from src.helper import argwrapper, imap_unordered_bar, transfer_to_s3

URL_HISTORICAL = 'https://api.airsafe.spire.com/archive/job?'
API_TOKEN = os.getenv('SPIRE_API_TOKEN')
S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME')
DIR_SAVE = 'data/output/spire/historical'
DIR_S3_PARENT='data/spire/historical'


def query_request(time_interval_start,
                  time_interval_stop,
                  icao_address=None,
                  callsign=None,
                  latitude_between=None,
                  longitude_between=None,
                  altitude_baro_between=None,
                  out_format='CSV',
                  compression=None,
                  ingestion_time_interval=None,
                  url_historical=URL_HISTORICAL,
                  api_token=API_TOKEN):
    """ query request to SPIRE historical database through AirSafe Historical API
    https://developer.airsafe.spire.com/get-started#apiHistorical
    Args:
        time_interval_start (datetime.pyi): start datetime of query time range
        time_interval_stop (datetime.pyi): end datetime of query time range
        icao_address (str): hexadecimal representation of ICAO 24-bit address, 6 characters, numbers 0-9 and upper case letters A-F
            ex: "02013F" or "02013F,0201xx"
        callsign (str): call sign
            ex: "RAM200" or "RAM200,ANA1"
        latitude_between (tuple): latitude in degrees, between -90 and 90 (both inclusive) -> tuple (y0, y1)
            ex: (-23, 12)
        longitude_between (tuple): 	longitude in degrees, between -180 (exclusive) and 180 (inclusive) -> tuple (x0, x1)
            ex: (-123, -100)
        altitude_baro_between (tuple): barometric altitude in feet, First value must be smaller than the second, specifies south-to-north range.
            First value is inclusive, last value is exclusive.
            ex: (33000, 34000)
            Cruising altitude is over 33000 ft
        out_format (str): Specifies the format of the downloadable files. Must be one of these options:
            “CSV” (encoded as UTF-8, and separated by a comma)
            “JSON” (encoded as UTF-8 and new line delimited)
            “AVRO” Default is CSV. JSON means new line delimited JSON.`
        compression (str): For CSV or JSON:GZIP, For AVRO:DEFLATE, SNAPPY
            If the parameter is not send or the string is empty, it defaults to no compression.
        ingestion_time_interval (tuple): timestamp Ingestion time records the timestamp when a record was made live into the database.
        url_historical (str): URL of historical API
        api_token (str): spire api token

    Returns: dictionary of job_state, job_id, api_token, headers

    """
    headers = {'Content-Type': 'application/json', 'Authorization': 'Bearer {0}'.format(api_token)}

    # time_interval
    time_interval_start_iso = time_interval_start.replace(microsecond=0).isoformat()
    time_interval_stop_iso = time_interval_stop.replace(microsecond=0).isoformat()
    time_interval = time_interval_start_iso + '/' + time_interval_stop_iso

    url = '{0}={1}'.format('time_interval', time_interval)
    if icao_address is not None:
        url = url + '&{0}={1}'.format('icao_address', icao_address)
    if callsign is not None:
        url = url + '&{0}={1}'.format('callsign', callsign)
    if callsign is not None:
        url = url + '&{0}={1}'.format('callsign', callsign)
    if latitude_between is not None:
        url = url + '&{0}={1}'.format('latitude_between', str(latitude_between[0]) + ',' + str(latitude_between[1]))
    if longitude_between is not None:
        url = url + '&{0}={1}'.format('longitude_between', str(longitude_between[0]) + ',' + str(longitude_between[1]))
    if altitude_baro_between is not None:
        url = url + '&{0}={1}'.format('longitude_between',
                                      str(int(altitude_baro_between[0])) + ',' + str(int(altitude_baro_between[1])))
    url = url + '&{0}={1}'.format('out_format', out_format)
    if compression is not None:
        url = url + '&{0}={1}'.format('compression', compression)
    if ingestion_time_interval is not None:
        ingestion_time_interval_start_iso = ingestion_time_interval[0].replace(microsecond=0).isoformat()
        ingestion_time_interval_stop_iso = ingestion_time_interval[1].replace(microsecond=0).isoformat()
        ingestion_time_interval = ingestion_time_interval_start_iso + '/' + ingestion_time_interval_stop_iso
        url = url + '{0}={1}'.format('ingestion_time_interval', ingestion_time_interval)
    # getting job_id for current call using put request
    response = requests.put(url_historical + url, headers=headers)
    putRes = response.content
    data = json.loads(putRes)
    job_id = data['job_id']
    job_state = data['job_state']
    print('API job id:', job_id)
    dit_out = {
        'job_state': job_state,
        'job_id': job_id,
        'api_token': api_token,
        'headers': headers,
        'url_query': url
    }
    return dit_out


def check_status(job_id, api_token, url_historical=URL_HISTORICAL):
    """ check status of query

    Args:
        job_id (str): job id
        api_token (str): spire api token
        url_historical (str): URL of historical API

    Returns: json

    """

    url_get = url_historical + 'job_id=' + job_id
    headers_get = {'Content-Type': 'application/json', 'Authorization': 'Bearer {0}'.format(api_token)}
    response_get = requests.get(url_get, headers=headers_get)
    data = json.loads(response_get.text)
    data1 = data['job_state']
    print('Job State: ', data1)
    return data


def get_data(job_id, api_token, url_historical=URL_HISTORICAL, max_wait_time=60, random_wait=True,
             dir_save=DIR_SAVE, filename='sample', out_format='CSV',
             save_s3=False, dir_s3_parent=DIR_S3_PARENT, remove_local_file=False, processes=1,
             s3_bucket_name=S3_BUCKET_NAME):
    """ get data from spire

    Args:
        job_id (str): job id
        api_token (str): spire api token
        url_historical (str): URL of historical API
        max_wait_time (int): maximum waiting interval (sec)
        dir_save (str): dir path for saving data
        filename (str): filename without ext
        out_format (str): Specifies the format of the downloadable files. Must be one of these options:
            “CSV” (encoded as UTF-8, and separated by a comma)
            “JSON” (encoded as UTF-8 and new line delimited)

    Returns: path to the download data

    """
    # data = check_status(job_id, api_token, url_historical=url_historical)
    wait_time = 0
    data = {
        'job_state': 'RUNNING'
    }

    while data['job_state'] != 'DONE':
        if (max_wait_time is not None) and (max_wait_time <= wait_time):
            wait_time = int(max_wait_time)
        else:
            wait_time += 15
        if random_wait:
            time.sleep(wait_time + random.randrange(-5, 5))
        else:
            time.sleep(wait_time)
        data = check_status(job_id, api_token, url_historical=url_historical)
        print('Job ID: ', job_id, '  Job State: ', data['job_state'])
        if data['job_state'] == 'DONE':
            dataurl = data['download_urls']
            dl_url = dataurl[0]
            # Get request to download data from URL and output it to a CSV file in current working directory.
            r = requests.get(dl_url, allow_redirects=True)
            path_temp = os.path.join(dir_save, filename)
            if out_format == 'CSV':
                path = path_temp + '.csv'
            elif out_format == 'JSON':
                path = path_temp + '.json'
            else:
                print('out_format should be CSV or JSON')
                return

            if not os.path.exists(dir_save):
                os.makedirs(dir_save)
            file = open(path, 'wb').write(r.content)

            if save_s3:
                path = transfer_to_s3(path, dir_local_parent=dir_save,
                                      dir_s3_parent=dir_s3_parent,
                                      remove_local_file=remove_local_file,
                                      multiprocessing=processes > 1, s3_bucket_name=s3_bucket_name)

    return path


class QueryGetManager(object):
    def __init__(self,
                 url_historical=URL_HISTORICAL,
                 api_token=API_TOKEN,
                 out_format='CSV',
                 compression=None,
                 ):
        self.url_historical = url_historical
        self.api_token = api_token
        self.out_format = out_format
        self.compression = compression

        self.list_dict = []

    def query_request(self,
                      time_interval_start,
                      time_interval_stop,
                      icao_address=None,
                      callsign=None,
                      latitude_between=None,
                      longitude_between=None,
                      altitude_baro_between=None,
                      ingestion_time_interval=None,
                      query_time_interval=None,
                      ):
        dict_args = {
            'icao_address': icao_address,
            'callsign': callsign,
            'latitude_between': latitude_between,
            'longitude_between': longitude_between,
            'altitude_baro_between': altitude_baro_between,
            'out_format': self.out_format,
            'compression': self.compression,
            'url_historical': self.url_historical,
            'api_token': self.api_token,
        }
        if query_time_interval is None:
            # dict_out = query_request(time_interval_start=time_interval_start,
            #                          time_interval_stop=time_interval_stop,
            #                          icao_address=icao_address,
            #                          callsign=callsign,
            #                          latitude_between=latitude_between,
            #                          longitude_between=longitude_between,
            #                          altitude_baro_between=altitude_baro_between,
            #                          out_format=self.out_format,
            #                          compression=self.compression,
            #                          ingestion_time_interval=ingestion_time_interval,
            #                          url_historical=self.url_historical,
            #                          api_token=self.api_token)
            dict_out = query_request(time_interval_start=time_interval_start,
                                     time_interval_stop=time_interval_stop,
                                     **dict_args)
            self.list_dict.append(dict_out)
        else:
            time_interval_start_temp = deepcopy(time_interval_start)
            time_interval_stop_temp = time_interval_start_temp + query_time_interval
            while time_interval_stop_temp < time_interval_stop:
                dict_out = query_request(time_interval_start=time_interval_start_temp,
                                         time_interval_stop=time_interval_stop_temp,
                                         **dict_args)
                self.list_dict.append(dict_out)
                time_interval_start_temp = time_interval_start_temp + query_time_interval
                time_interval_stop_temp = time_interval_start_temp + query_time_interval

            time_interval_stop_temp = deepcopy(time_interval_stop)
            dict_out = query_request(time_interval_start=time_interval_start_temp,
                                     time_interval_stop=time_interval_stop_temp,
                                     **dict_args)
            self.list_dict.append(dict_out)
        return self.list_dict

    def get_data_bulk(self, max_wait_time=60, random_wait=True, dir_save=DIR_SAVE, processes=1,
                      save_s3=False, dir_s3_parent=DIR_S3_PARENT, remove_local_file=False,
                      s3_bucket_name=S3_BUCKET_NAME):
        if processes == 1:
            list_path = []
            for dict_out in tqdm(self.list_dict, total=len(self.list_dict)):
                path = get_data(job_id=dict_out['job_id'], api_token=dict_out['api_token'],
                                url_historical=self.url_historical,
                                max_wait_time=max_wait_time, random_wait=random_wait, dir_save=dir_save,
                                filename=dict_out['url_query'].replace('/', 'to'), out_format=self.out_format,
                                save_s3=save_s3, dir_s3_parent=dir_s3_parent, remove_local_file=remove_local_file,
                                processes=processes,
                                s3_bucket_name=s3_bucket_name)
                list_path.append(path)
        else:
            func_args = [(get_data, dict_out['job_id'], dict_out['api_token'], self.url_historical, max_wait_time,
                          random_wait, dir_save, dict_out['url_query'].replace('/', 'to'), self.out_format,
                          save_s3, dir_s3_parent, remove_local_file, processes, s3_bucket_name)
                         for dict_out in self.list_dict]
            list_path = imap_unordered_bar(argwrapper, func_args, processes, extend=False)

        return list_path


def test_defs():
    time_interval_start = datetime.datetime(year=2019, month=9, day=1, hour=0, minute=0, second=0, tzinfo=pytz.utc)
    time_interval_stop = datetime.datetime(year=2019, month=9, day=1, hour=0, minute=0, second=3, tzinfo=pytz.utc)
    callsign = None

    dict_out = query_request(time_interval_start=time_interval_start,
                             time_interval_stop=time_interval_stop,
                             callsign=callsign)
    # print(dict_out)
    # dict_out = {'job_state': 'RUNNING', 'job_id': 'RuR0kgVgSFv6kYtWoEAf9hmF9ew__CSV_0',
    #             'api_token': 'yLJv4KKhhdaK6juoZNKWK2Cxbl7Sqwh3',
    #             'headers': {'Content-Type': 'application/json',
    #                         'Authorization': 'Bearer yLJv4KKhhdaK6juoZNKWK2Cxbl7Sqwh3'}}
    check_status(job_id=dict_out['job_id'],
                 api_token=dict_out['api_token'])
    get_data(job_id=dict_out['job_id'], api_token=dict_out['api_token'])


def main():
    time_interval_start = datetime.datetime(year=2019, month=9, day=1, hour=0, minute=1, second=0, tzinfo=pytz.utc)
    time_interval_stop = datetime.datetime(year=2019, month=9, day=1, hour=0, minute=1, second=4, tzinfo=pytz.utc)
    query_time_interval = datetime.timedelta(seconds=1)
    save_s3 = True
    remove_local_file = False
    processes = 3

    query_get_manager = QueryGetManager()
    list_dict = query_get_manager.query_request(time_interval_start,
                                                time_interval_stop,
                                                icao_address=None,
                                                callsign=None,
                                                latitude_between=None,
                                                longitude_between=None,
                                                altitude_baro_between=None,
                                                ingestion_time_interval=None,
                                                query_time_interval=query_time_interval,
                                                )
    print(list_dict)
    list_path = query_get_manager.get_data_bulk(max_wait_time=60,
                                                random_wait=True,
                                                processes=processes,
                                                save_s3=save_s3,
                                                remove_local_file=remove_local_file)
    print(list_path)


if __name__ == '__main__':
    main()
