import datetime

from flight_data_gathering.open_sky.flight_info import HistoricalDataManager

if __name__ == '__main__':
    # only date and icao24 code:
    start_datetime = datetime.datetime(year=2018, month=11, day=14, hour=6)
    stop_datetime = datetime.datetime(year=2018, month=11, day=14, hour=7)

    # paramters for Impala.history
    icao24 = '861ede'
    limit = 10

    # temp
    file_batch_unit = 'daily'
    time_interval = datetime.timedelta(minutes=1)

    historical_data_manager = HistoricalDataManager(file_batch_unit=file_batch_unit,
                                                    time_interval=time_interval
                                                    )
    df = historical_data_manager.get_data_single_date_test(start_datetime=start_datetime,
                                                           stop_datetime=stop_datetime,
                                                            icao24=icao24,
                                                           limit=limit)
    print(df)
