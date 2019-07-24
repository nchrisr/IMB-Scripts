import pymongo
import pandas as pd
import csv
import traceback

IMB_IDS = ["CEOS_IMBB01", "CEOS_IMBB02", "CEOS_IMBB04", "BREA_IMB1", "BREA_IMB2", "Daneborg_IMB1",
         "Daneborg_IMB2", "StnNord_IMB1", "StnNord_IMB2"]

HEADERS_ORDER1 = [""]
HEADERS_ORDER2 = []
HEADERS_ORDER3 = ["Device_Datetime_UTC", "GPS_Hour", "GPS_Minute", "GPS_Second", "Year", "Month", "Day",
                  "Hour", "Minute", "Seconds", "GPGGA", "Latitude_dd", "Longitude_dd", "Quality Indicator",
                  "Number of Satellites Used", "HDOP", "Antenna altitude", "altitude_units", "Geoidal Separation",
                  "Geoidal Separation Units", "Correction age", "Checksum", "Date Logger temperature", "Battery Voltage",
                  "Battery voltage", "Air temperature", "Sea Level Pressure", "Raw under water sounder distance",
                  "UW sounder distance", "Raw snow sounder distance", "Snow sounder quality",
                  "Corrected Snow sounder distance", "Thermistor Temperature-1", "Thermistor Temperature-2",
                  "Thermistor Temperature-3", "Thermistor Temperature-12", "Thermistor Temperature-13",
                  "Thermistor Temperature-14", "Thermistor Temperature-15", "Thermistor Temperature-16", "Thermistor Temperature-17",
                  "Thermistor Temperature-18", "Thermistor Temperature-19", "Thermistor Temperature-2" ,"Thermistor Temperature-20",
                  "Thermistor Temperature-21", "Thermistor Temperature-22", "Thermistor Temperature-23", "Thermistor Temperature-24" ,
                  "Thermistor Temperature-25", "Thermistor Temperature-26","Thermistor Temperature-27",
                  "Thermistor Temperature-28" ,"Thermistor Temperature-29", "Thermistor Temperature-3" , "Thermistor Temperature-30" ,
                  "Thermistor Temperature-31", "Thermistor Temperature-32", "Thermistor Temperature-33",
                  "Thermistor Temperature-34", "Thermistor Temperature-35", "Thermistor Temperature-36",
                  "Thermistor Temperature-37", "Thermistor Temperature-38","Thermistor Temperature-39",
                  "Thermistor Temperature-4",
                  "Thermistor Temperature-40", "Thermistor Temperature-41", "Thermistor Temperature-42",
                  "Thermistor Temperature-43", "Thermistor Temperature-44", "Thermistor Temperature-45",
                  "Thermistor Temperature-46", "Thermistor Temperature-47",
                  "Thermistor Temperature-48", "Thermistor Temperature-49",
                  "Thermistor Temperature-5", "Thermistor Temperature-50", "Thermistor Temperature-51",
                  "Thermistor Temperature-52", "Thermistor Temperature-53", "Thermistor Temperature-54",
                  "Thermistor Temperature-55","Thermistor Temperature-56", "Thermistor Temperature-57",
                  "Thermistor Temperature-58", "Thermistor Temperature-59",
                  "Thermistor Temperature-6", "Thermistor Temperature-60", "Thermistor Temperature-61",
                  "Thermistor Temperature-62","Thermistor Temperature-63", "Thermistor Temperature-64",
                  "Thermistor Temperature-65", "Thermistor Temperature-66", "Thermistor Temperature-67",
                  "Thermistor Temperature-68", "Thermistor Temperature-7", "Thermistor Temperature-8",
                  "Thermistor Temperature-9", "connection_string", "imb_id", "original_file_name",
                  "processed_file_name"
                  ]

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


def convert_geos(value, cardinal_point):
    geo_string = str(value)
    decimal_index = geo_string.index('.')
    minute_start_index = decimal_index-2
    d = geo_string[0:minute_start_index]
    minutes = geo_string[minute_start_index:]
    minutes_in_decimal = float(minutes)/60
    if cardinal_point.lower() == "w" or cardinal_point.lower() == "s":
        return_value = -1*(float(d)+minutes_in_decimal)
    else:
        return_value = float(d)+minutes_in_decimal

    return return_value


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
    long_cardinal_point = row["longitude_cardinal_point"]
    new_longitude = None
    if long is not None:
        try:
            new_longitude = convert_geos(long, long_cardinal_point)
        except Exception as e:
            print(e)
            print ("Converting longitude to decimal degrees failed, due to invalid value.")
    row["Longitude_dd"] = new_longitude
    del row["Longitude_ddmm"]
    del row["longitude_cardinal_point"]

    lat = row["Latitude_ddmm"]
    lat_cardinal_point = row["latitude_cardinal_point"]
    new_latitude = None
    if lat is not None:
        try:
            new_latitude = convert_geos(lat, lat_cardinal_point)
        except Exception as e:
            print(e)
            print("Converting latitude to decimal degrees failed, due to invalid value.")
    row["Latitude_dd"] = new_latitude
    del row["Latitude_ddmm"]
    del row["latitude_cardinal_point"]

    con_string = row['metadata']["connection_string"]
    imb_id = row["metadata"]["imb_id"]
    procesed_file = row["metadata"]["processed_file"]
    original_file = row["metadata"]["original_file_name"]
    row["connection_string"] = con_string
    row["imb_id"] = imb_id
    row["processed_file_name"] = procesed_file
    row["original_file_name"] = original_file

    del row["metadata"]
    del row["_id"]

    return row


def clean_data(data):
    count = 0
    for row in data:
        try:
            row = clean_data_row(row)
        except Exception as e:
            print(count)
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
        df = df[[]+HEADERS_ORDER3]
        df.to_csv(id+".csv", index=False)
