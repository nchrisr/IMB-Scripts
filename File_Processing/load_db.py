import pymongo
import csv
import pandas as pd
import StringIO
import sys
import pathlib2
import os
import re
import datetime
import numpy as np

# This dictionary is a representation of the directory structure which should be generated before running this script
# on the specified working directory.
DIRECTORY_TREES = {"01": [2009, 2010], "02": [2010, 2011, 2012, 2013], "03": [2013, 2014, 2015, 2016]}

WORKING_DIRECTORY = None
if len(sys.argv) < 2:
    print("Incomplete Arguments... at lease one argument required")
    exit(-1)
else:
    WORKING_DIRECTORY = sys.argv[1]

CSV_EXTENSION = '.csv'
DATABASE = "Ice_Mass_Buoy_Data"
COLLECTION = "Series"
OUTPUTS_FOLDER = "Outputs"  # outputs folder if files are located there.

# List of symbols to remove
BAD_SYMBOLS = ["-", "'", '"']

# Template for Therm data headers.
THERMISTOR_TEMPERATURE_HEADERS = "Thermistor Temperature-"


def clean_data(data_file):
    original_file = str((pathlib2.Path(data_file).parent).name)
    cleaned_file = str(pathlib2.Path(data_file).name)
    dframe = pd.read_csv(data_file, skiprows=6, dtype=str)
    metadata = pd.read_csv(data_file, nrows=4, header=None)
    metadata_dictionary = {}
    for index, current_row in metadata.iterrows():
        key = ((str(current_row[0])).lower()).replace(' ', '_')
        value = str(current_row[1])

        if key == "rings":
            value = int(value)
        elif key == "connection_string":
            if not(bool(value.strip())):
                value = None
            else:
                if value.strip() == "None":
                    value = None
        elif key == "imb_id":
            if not(bool(value.strip())):
                value = None
            else:
                if value.strip() == "None":
                    value = None
        elif key == "transmission_finished_successfully":
            if value.strip() == "True":
                value = True
            else:
                value = False
        elif key == "data_line_after_connection_string":
            if not(bool(value.strip())):
                value = None
            else:
                if value.strip() == "None":
                    value = None

        metadata_dictionary[key] = value
        print("H")
    metadata_dictionary["original_file_name"] = original_file
    metadata_dictionary["processed_file"] = cleaned_file
    print(metadata_dictionary)


    #dframe.replace("nan", np.nan, inplace=True)
    #dframe.replace(to_replace=[None], value=np.nan, inplace=True)
    csv_str = dframe.to_csv()
    reader = csv.DictReader(StringIO.StringIO(csv_str))

    data_list = []
    for row in reader:
        del row['']
        print row
        for key in row:
            if row[key] in BAD_SYMBOLS:
                row[key] = None
            if key == "Device_Datetime_UTC":
                try:
                    row[key] = datetime.datetime.strptime(row[key], "%Y-%m-%d %H:%M:%S")
                except Exception as e:
                    try:
                        row[key] = datetime.datetime.strptime(row[key], '%Y-%m-%d %H:%M:%S"')
                    except Exception as e:
                        row[key] = None
            if key == "$GPGGA":
                try:
                    if row[key] != "$GPGGA":
                        row["$GPGGA"] = None
                except Exception as e:
                    row[key] = None
            elif key == "GPS_Time_hhmmss":
                try:
                    row[key] = int(row[key])
                    if len(((str(row[key])).split('.'))[0]) != 6:
                        row[key] = None
                except Exception as e:
                    row[key] = None
            elif key == "Latitude_degrees_decimal_minutes_ddmm.mmmm":
                try:
                    row[key] = float(row[key])
                except Exception as e:
                    row[key] = None
            elif key == "N/S":
                try:
                    if row[key].lower() != "n" and row[key].lower() != "s":
                        row["key"] = None
                except Exception as e:
                    row[key] = None
            elif key == "Longitude_degrees_decimal_minutes_ddmm.mmmm":
                try:
                    row[key] = float(row[key])
                except Exception as e:
                    row[key] = None
            elif key == "W/E":
                try:
                    if row[key].lower() != "w" and row[key].lower() != "e":
                        row["key"] = None
                except Exception as e:
                    row[key] = None
            elif key == "Quality Indicator":
                try:
                    row[key] = float(row[key])
                except Exception as e:
                    row[key] = None
            elif key == "Number of Satellites Used":
                try:
                    row[key] = int(row[key])
                except Exception as e:
                    row[key] = None
            elif key == "HDOP(horizontal dilution of precision)":
                try:
                    row[key] = float(row[key])
                except Exception as e:
                    row[key] = None
            elif key == "Antenna altitude":
                try:
                    row[key] = float(row[key])
                except Exception as e:
                    row[key] = None
            elif key == "altitude units (M(metres)/ F(feet))":
                try:
                    if row[key].lower() != "m" and row[key].lower() != "f":
                        row["key"] = None
                except Exception as e:
                    row[key] = None
            elif key == "Geoidal Separation":
                try:
                    row[key] = float(row[key])
                except Exception as e:
                    row[key] = None
            elif key == "Geoidal Separation Units (M(metres)/ F(feet))":
                try:
                    if row[key].lower() != "m" and row[key].lower() != "f":
                        row["key"] = None
                except Exception as e:
                    row[key] = None
            elif key == "Checksum":
                try:
                    if not(bool(re.search("(\*\d)(\d|[a-zA-Z])", str(row[key])))):
                        row[key] = None
                except Exception as e:
                    row[key] = None
            elif key == "Year":
                try:
                    row[key] = int(row[key])
                except Exception as e:
                    row[key] = None
            elif key == "Month":
                try:
                    row[key] = int(row[key])
                    if not(1 <= int(row[key]) <= 12):
                        row[key] = None
                except Exception as e:
                    row[key] = None

            elif key == "Day":
                try:
                    row[key] = int(row[key])
                    if not (1 <= int(row[key]) <= 31):
                        row[key] = None
                except Exception as e:
                    row[key] = None
            elif key == "Day of Year":
                try:
                    row[key] = int(row[key])
                    if not(1 <= row[key] <= 365):
                        row[key] = None
                except Exception as e:
                    row[key] = None
            elif key == "Hour":
                try:
                    row[key] = int(row[key])
                    if not(0 <= int(row[key]) <= 23):
                        row[key] = None
                except Exception as e:
                    row[key] = None
            elif key == "Minute":
                try:
                    if not(0 <= int(row[key]) <= 23):
                        row[key] = None
                except Exception as e:
                    row[key] = None
            elif key == "Seconds":
                try:
                    row[key] = int(row[key])
                except Exception as e:
                    row[key] = None
            elif key == "Date Logger temperature" or key == "Battery voltage" or key == "Air temperature" or key == "Sea Level Pressure" or key=="Battery Voltage":
                try:
                    row[key] = float(row[key])
                except Exception as e:
                    row[key] = None
            elif key == "Raw under water sounder distance":
                try:
                    if not(bool(re.search('^\R\d+\.?\d*$', row[key]))):
                        row[key] = None
                except Exception as e:
                    row[key] = None
            elif key == "UW sounder distance" or key == "Raw snow sounder distance" or key == "Raw sounder quality" or key == "Corrected Snow sounder distance" or key == "Snow sounder quality":
                try:
                    row[key] = float(row[key])
                except Exception as e:
                    row[key] = None

            elif THERMISTOR_TEMPERATURE_HEADERS in key:
                try:
                    row[key] = float(row[key])
                except Exception as e:
                    row[key] = None

        gpgga = row["$GPGGA"]
        del row["$GPGGA"]
        row["GPGGA"] = gpgga

        latitude = row["Latitude_degrees_decimal_minutes_ddmm.mmmm"]
        del row["Latitude_degrees_decimal_minutes_ddmm.mmmm"]
        row["Latitude_ddmm"] = latitude

        n_s = row["N/S"]
        del row["N/S"]
        row["latitude_cardinal_point"] = n_s

        longitude = row["Longitude_degrees_decimal_minutes_ddmm.mmmm"]
        del row["Longitude_degrees_decimal_minutes_ddmm.mmmm"]
        row["Longitude_ddmm"] = longitude

        w_e = row["W/E"]
        del row["W/E"]
        row["longitude_cardinal_point"] = w_e

        hdop = row["HDOP(horizontal dilution of precision)"]
        del row["HDOP(horizontal dilution of precision)"]
        row["HDOP"] = hdop

        alt_units = row["altitude units (M(metres)/ F(feet))"]
        del row["altitude units (M(metres)/ F(feet))"]
        row["altitude_units"] = alt_units

        geoidal_units = row["Geoidal Separation Units (M(metres)/ F(feet))"]
        del row["Geoidal Separation Units (M(metres)/ F(feet))"]
        row["Geoidal Separation Units"] = geoidal_units

        row["metadata"] = metadata_dictionary
        data_list.append(row)

    return data_list


def load_into_database(data_set):
    client = pymongo.MongoClient()
    db = client[DATABASE]
    collection = db[COLLECTION]
    output = collection.insert_many(data_set)

    return output


def do_process(working_directory=WORKING_DIRECTORY):
    """This function will go down to the the outputs directory within the sub directories in the 'working_directory'
    argument.

    It will call the second_imb_process() function to process each file."""

    # Use the directory tree dictionary to determine the directory to search for data in, and process each file.
    for key in DIRECTORY_TREES:
        for year in DIRECTORY_TREES[key]:
            current_directory = str(pathlib2.Path(working_directory, key, str(year), OUTPUTS_FOLDER))
            list_of_directories = os.listdir(current_directory)
            for directory in list_of_directories:
                full_dir_path = str(pathlib2.Path(current_directory, directory))
                if (pathlib2.Path(full_dir_path)).is_dir():
                    files_to_process = os.listdir(full_dir_path)
                    for curr_file in files_to_process:
                        if curr_file.endswith(CSV_EXTENSION):
                            file_path = str(pathlib2.Path(full_dir_path, curr_file))
                            cleaned_data = clean_data(file_path)
                            load_into_database(cleaned_data)

                else:
                    print("Invalid directory path {}".format(full_dir_path))
    return


do_process()

print("End of Processing...")


