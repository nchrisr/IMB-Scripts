import pymongo
import pandas as pd
import csv
import traceback

IMB_IDS = ["CEOS_IMBB01", "CEOS_IMBB02", "CEOS_IMBB04", "BREA_IMB1", "BREA_IMB2", "Daneborg_IMB1",
         "Daneborg_IMB2", "StnNord_IMB1", "StnNord_IMB2"]

DATABASE = "Ice_Mass_Buoy_Data"
COLLECTION = "Series"


def get_data(imb_id):
    client = pymongo.MongoClient()
    db = client[DATABASE]
    collection = db[COLLECTION]
    data = collection.find({"metadata.imb_id": imb_id})
    data_list = []
    for datum in data:
        data_list.append(datum)

    return data_list


def convert_geos(value):
    geo_string = str(value)
    decimal_index = geo_string.index('.')
    minute_start_index = decimal_index-2
    d = geo_string[0:minute_start_index]
    minutes = geo_string[minute_start_index:]
    minutes_in_decimal = float(minutes)/60

    return float(d)+minutes_in_decimal


def clean_data_row(row):

    gps_time = row["GPS_Time_hhmmss"]
    gps_hours = None
    gps_minutes = None
    gps_seconds = None
    if gps_time is not None:
        gps_time = str(gps_time)
        gps_hours = int(gps_time[0:2])
        gps_minutes = int(gps_time[2:4])
        gps_seconds = float(gps_time[4:])
    row["GPS_Hour"] = gps_hours
    row["GPS_Minute"] = gps_minutes
    row["GPS_Second"] = gps_seconds
    del row["GPS_Time_hhmmss"]

    long = row["Longitude_ddmm"]
    new_longitude = None
    if row["Longitude_ddmm"] is not None:
        new_longitude = convert_geos(long)
    row["Longitude_dd"] = new_longitude
    del row["Longitude_ddmm"]

    lat = row["Latitude_ddmm"]
    new_latitude = None
    if row["Latitude_ddmm"] is not None:
        new_latitude = convert_geos(lat)
    row["Latitude_dd"] = new_latitude
    del row["Latitude_ddmm"]

    del row["_id"]

    return row


def clean_data(data):
    count = 0
    for row in data:
        """if count >= 111:
            print("H")
        print(count)"""
        try:
            row = clean_data_row(row)
        except Exception as e:
            print("Processing of data row failed")
            print(e)
            print(row)
            traceback.print_exc()
            print('\n')

        count += 1
    return data


for id in IMB_IDS:
    if id == "BREA_IMB1":
        data = get_data(id)
        data = clean_data(data)
        df = pd.DataFrame(data)
        df.to_csv(id+".csv")
