import pymongo
import pandas as pd
import traceback
import pathlib2
import os

# Database connection stuff
DATABASE = "Ice_Mass_Buoy_Data"
COLLECTION = "Series"
MONGO_CLIENT = pymongo.MongoClient()
DB = MONGO_CLIENT[DATABASE]
COLL = DB[COLLECTION]

# Query the database to get a list of all different imb_id's in the database
IMB_IDS = COLL.distinct("metadata.imb_id")

# A list of all the imb_id's that are expected to be in the database.
EXPECTED_IDS = ["CEOS_IMBB01", "CEOS_IMBB02", "CEOS_IMBB04", "BREA_IMB1", "BREA_IMB2", "Daneborg_IMB1",
                "Daneborg_IMB2", "StnNord_IMB1", "StnNord_IMB2"]

# Constants needed for cleaning the data.
GEO_DECIMAL_PLACES = 4
LOWER_LAT = 49
UPPER_LAT = 50

LOWER_LONG = -97.9
UPPER_LONG = -96.5

ALL_HEADERS_ORDERED_LIST = ["connection_string", "imb_id", "original_file_name","processed_file_name",
                            "Device_Datetime_UTC", "GPGGA", "GPS_Hour", "GPS_Minute", "GPS_Second", "Latitude_dd",
                            "Longitude_dd","Quality Indicator", "Number of Satellites Used", "HDOP", "Antenna altitude",
                            "altitude_units","Geoidal Separation", "Geoidal Separation Units", "Correction age",
                            "Checksum","Year", "Month", "Day", "Day of year","Hour", "Minute", "Seconds",
                            "Date Logger temperature","Battery Voltage", "Battery Voltage_Units", "Air temperature",
                            "Sea Level Pressure","Sea Level Pressure_Units","Raw under water sounder distance",
                            "UW sounder distance", "Raw snow sounder distance","Snow sounder quality",
                            "Corrected Snow sounder distance",
                            "Thermistor Temperature-1","Thermistor Temperature-2", "Thermistor Temperature-3",
                            "Thermistor Temperature-4","Thermistor Temperature-5","Thermistor Temperature-6",
                            "Thermistor Temperature-7", "Thermistor Temperature-8","Thermistor Temperature-9",
                            "Thermistor Temperature-10", "Thermistor Temperature-11","Thermistor Temperature-12",
                            "Thermistor Temperature-13", "Thermistor Temperature-14","Thermistor Temperature-15",
                            "Thermistor Temperature-16", "Thermistor Temperature-17","Thermistor Temperature-18",
                            "Thermistor Temperature-19", "Thermistor Temperature-20","Thermistor Temperature-21",
                            "Thermistor Temperature-22", "Thermistor Temperature-23","Thermistor Temperature-24",
                            "Thermistor Temperature-25", "Thermistor Temperature-26","Thermistor Temperature-27",
                            "Thermistor Temperature-28", "Thermistor Temperature-29","Thermistor Temperature-30",
                            "Thermistor Temperature-31", "Thermistor Temperature-32","Thermistor Temperature-33",
                            "Thermistor Temperature-34", "Thermistor Temperature-35","Thermistor Temperature-36",
                            "Thermistor Temperature-37", "Thermistor Temperature-38","Thermistor Temperature-39",
                            "Thermistor Temperature-40", "Thermistor Temperature-41","Thermistor Temperature-42",
                            "Thermistor Temperature-43", "Thermistor Temperature-44","Thermistor Temperature-45",
                            "Thermistor Temperature-46", "Thermistor Temperature-47","Thermistor Temperature-48",
                            "Thermistor Temperature-49", "Thermistor Temperature-50","Thermistor Temperature-51",
                            "Thermistor Temperature-52", "Thermistor Temperature-53","Thermistor Temperature-54",
                            "Thermistor Temperature-55", "Thermistor Temperature-56","Thermistor Temperature-57",
                            "Thermistor Temperature-58", "Thermistor Temperature-59","Thermistor Temperature-60",
                            "Thermistor Temperature-61", "Thermistor Temperature-62","Thermistor Temperature-63",
                            "Thermistor Temperature-64", "Thermistor Temperature-65","Thermistor Temperature-66",
                            "Thermistor Temperature-67", "Thermistor Temperature-68", "Thermistor Temperature-69",
                            "Thermistor Temperature-70", "Thermistor Temperature-71", 'Thermistor Temperature-72',
                            'Thermistor Temperature-73', 'Thermistor Temperature-74', 'Thermistor Temperature-75',
                            'Thermistor Temperature-76', 'Thermistor Temperature-77', 'Thermistor Temperature-78',
                            'Thermistor Temperature-79', 'Thermistor Temperature-80', 'Thermistor Temperature-81',
                            'Thermistor Temperature-82', 'Thermistor Temperature-83', 'Thermistor Temperature-84',
                            'Thermistor Temperature-85', 'Thermistor Temperature-86', 'Thermistor Temperature-87',
                            'Thermistor Temperature-88', 'Thermistor Temperature-89', 'Thermistor Temperature-90',
                            'Thermistor Temperature-91', 'Thermistor Temperature-92', 'Thermistor Temperature-93',
                            'Thermistor Temperature-94', 'Thermistor Temperature-95', 'Thermistor Temperature-96',
                            'Thermistor Temperature-97', 'Thermistor Temperature-98', 'Thermistor Temperature-99',
                            'Thermistor Temperature-100', 'Thermistor Temperature-101', 'Thermistor Temperature-102',
                            'Thermistor Temperature-103', 'Thermistor Temperature-104', 'Thermistor Temperature-105',
                            'Thermistor Temperature-106', 'Thermistor Temperature-107', 'Thermistor Temperature-108',
                            'Thermistor Temperature-109', 'Thermistor Temperature-110', 'Thermistor Temperature-111',
                            'Thermistor Temperature-112', 'Thermistor Temperature-113', 'Thermistor Temperature-114',
                            'Thermistor Temperature-115', 'Thermistor Temperature-116', 'Thermistor Temperature-117',
                            'Thermistor Temperature-118', 'Thermistor Temperature-119', "Thermistor Temperature-120"]

OUTPUTS_FOLDER = "Outputs"


def get_data(imb_id):
    """ Get all the rows of data for which were gotten by the IMB specified by the 'imb_id'."""
    # connect to database and run query
    client = pymongo.MongoClient()
    db = client[DATABASE]
    collection = db[COLLECTION]
    data_set = collection.find({"metadata.imb_id": imb_id})
    data_list = []
    # Add all the data to a list and return the list.
    for datum in data_set:
        data_list.append(datum)

    return data_list


def convert_geos(value, cardinal_point):
    """
    Converts 'value' which is a geo point value in ddmm.mmmm format into decimal degrees
    Needs to consider the cardinal_point to determine if it should be negative"""

    # convert to string and change to degrees decimal.
    geo_string = str(value)
    decimal_index = geo_string.index('.')
    minute_start_index = decimal_index-2
    d = geo_string[0:minute_start_index]
    minutes = geo_string[minute_start_index:]
    minutes_in_decimal = float(minutes)/60

    # convert to negative if its west of south.
    if cardinal_point is not None:
        if cardinal_point.lower() == "w" or cardinal_point.lower() == "s":
            return_value = -1*(float(d)+minutes_in_decimal)
        else:
            return_value = float(d) + minutes_in_decimal
    else:
        return_value = float(d)+minutes_in_decimal

    return return_value


def clean_data_row(row):
    """
    Clean all the fields in row. Break up the GPS time into separate fields
    Set all fields that can't be cleaned to be None
    Convert all the long and lat values into degrees decimal
    """
    is_test_data = False

    gps_time = row["GPS_Time_hhmmss"]
    gps_hours = None
    gps_minutes = None
    gps_seconds = None
    if gps_time is not None:
        gps_time = str(gps_time)
        gps_time = gps_time.replace('{', '')
        gps_time = gps_time.replace('}', '')
        char_count = len(gps_time.split('.')[0])
        if char_count < 6:
            # If there's less than six(6) characters in the GPS time field pad zeros in front of it.
            zeros_to_add = "0"*(6-char_count)
            gps_time = zeros_to_add+gps_time
        gps_hours = int(gps_time[0:2])
        gps_minutes = int(gps_time[2:4])
        gps_seconds = float(gps_time[4:])
    row["GPS_Hour"] = gps_hours
    row["GPS_Minute"] = gps_minutes
    row["GPS_Second"] = gps_seconds
    # Delete the GPS time field
    del row["GPS_Time_hhmmss"]

    # Convert long and lat values and delete the original long and lat fields
    # Also check using the defined range to see if the data was collected in Winnipeg which means it is test data.
    long = row["Longitude_ddmm"]
    long_cardinal_point = row["longitude_cardinal_point"]
    new_longitude = None
    if long is not None:
        try:
            new_longitude = convert_geos(long, long_cardinal_point)
            # Check if it is is test data
            if LOWER_LONG <= new_longitude <= UPPER_LONG:
                is_test_data = True
        except Exception as e:
            print ("Converting longitude to decimal degrees failed, due to invalid value.")
    if new_longitude is not None:
        new_longitude = round(new_longitude, GEO_DECIMAL_PLACES)
    row["Longitude_dd"] = new_longitude
    del row["Longitude_ddmm"]
    del row["longitude_cardinal_point"]

    lat = row["Latitude_ddmm"]
    lat_cardinal_point = row["latitude_cardinal_point"]
    new_latitude = None
    if lat is not None:
        try:
            new_latitude = convert_geos(lat, lat_cardinal_point)
            # Check if it is is test data
            if LOWER_LAT <= new_latitude <= UPPER_LAT:
                is_test_data = True
        except Exception as e:
            print("Converting latitude to decimal degrees failed, due to invalid value.")
    if new_latitude is not None:
        new_latitude = round(new_latitude, GEO_DECIMAL_PLACES)
    row["Latitude_dd"] = new_latitude
    del row["Latitude_ddmm"]
    del row["latitude_cardinal_point"]

    # Get the metadata fields and make them fields of their own
    con_string = row['metadata']["connection_string"]
    imb_id = row["metadata"]["imb_id"]
    procesed_file = row["metadata"]["processed_file"]
    original_file = row["metadata"]["original_file_name"]
    row["connection_string"] = con_string
    row["imb_id"] = imb_id
    row["processed_file_name"] = procesed_file
    row["original_file_name"] = original_file

    # Add the fields for the units.
    row["Sea Level Pressure_Units"] = "mb"
    row["Battery Voltage_Units"] = "V"

    del row["metadata"]
    del row["_id"]
    if "Battery Voltage-2" in row:
        del row["Battery Voltage-2"]

    # return a dictionary with the row key being the data that was found to be actual data, and the "test-data" key
    # being data that was found to be test data.
    return {"row": row, "test-data": is_test_data}


def clean_data(current_data):
    """
    This function makes calls to other functions and cleans all the rows of data in 'current_data'
    :param current_data: A list of dictionaries; each dictionary represents a row of data
    :return: A dictionary with two keys
             'test_list' is a list of dictionaries with each dictionary representing a row of test data.
             'non_test_list' is a list of dictionaries with each dictionary representing a row of actual data
    """
    count = 0
    non_test_list = []
    test_list = []
    for index in range(0, len(current_data)):
        row = current_data[index]
        try:
            output = clean_data_row(row)
            row = output["row"]
            if output["test-data"]:
                test_list.append(current_data[index])
            else:
                non_test_list.append(current_data[index])

        except Exception as e:
            print(count)
            print(row)
            traceback.print_exc()
            print('\n')

        count += 1

    return {"test_list": test_list, "non_test_list": non_test_list}

# make outputs folder if it doesnt already exist.
try:
    os.mkdir(OUTPUTS_FOLDER)
except Exception as e:
    print(" \nOutputs directory already exists...")

test_data = []  # list of rows which are test data
for curr_id in IMB_IDS:
    # For each of the ID's in the database, get its data from the database, clean it, make a dataframe from the data,
    # Make a list of headers for the data (so that the order is specified)

    print("\nRunning process for imb_id: {}\n".format(curr_id))
    data = get_data(curr_id)
    data_dictionary = clean_data(data)
    test_data += data_dictionary["test_list"]
    df = pd.DataFrame(data_dictionary["non_test_list"])
    columns = df.columns

    # For each of the headers in the list of all the possible headers, if that headers is in the dataframe,
    #  add it to the headers to use variable
    headers_to_use = [] # will be used to specify the order for the headers in the outputed data.
    for column_name in ALL_HEADERS_ORDERED_LIST:
        if column_name in columns:
            headers_to_use.append(column_name)

    # Specify the order of headers for the dataframe
    # Sort the data by datetime and output a csv file.
    df = df[headers_to_use]
    df['Device_Datetime_UTC'] = pd.to_datetime(df['Device_Datetime_UTC'])
    df = df.sort_values(by='Device_Datetime_UTC')

    save_path = str(pathlib2.Path(OUTPUTS_FOLDER, str(curr_id)+'.csv'))
    df.to_csv(save_path, index=False)

# Make a dataframe from the test data, sort it by Datetime, make the headers and make a csv file for that.
df = pd.DataFrame(test_data)
df['Device_Datetime_UTC'] = pd.to_datetime(df['Device_Datetime_UTC'])
df = df.sort_values(by='Device_Datetime_UTC')
columns = df.columns
headers_to_use = []

for column_name in ALL_HEADERS_ORDERED_LIST:
    if column_name in columns:
        headers_to_use.append(column_name)
df = df[headers_to_use]
save_path = str(pathlib2.Path("Outputs", "TEST-DATA"+'.csv'))
df.to_csv(save_path, index=False)








