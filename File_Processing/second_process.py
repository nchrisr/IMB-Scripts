import sys
import os
import pathlib2
import shutil
import StringIO
import pandas as pd

# This dictionary is a representation of the directory structure which should be generated before running this script
# on the specified working directory.
DIRECTORY_TREES = {"01": [2009, 2010], "02": [2011, 2012, 2013], "03": [2014, 2015, 2016]}

# Headers for the GPS data
GPS_HEADERS = ["$GPGGA", "GPS_Time_hhmmss", "Latitude_degrees_decimal_minutes_ddmm.mmmm", "N/S",
               "Longitude_degrees_decimal_minutes_ddmm.mmmm", "W/E", "Quality Indicator", "Number of Satellites Used",
               "HDOP(horizontal dilution of precision)", "Antenna altitude", "altitude units (M(metres)/ F(feet))",
               "Geoidal Separation", "Geoidal Separation Units (M(metres)/ F(feet))", "Correction age",
               "Checksum", "Unknown", "Unknown", "Unknown"]

# Headers for Output1 data
OUTPUT_ONE_HEADERS = ["Year", "Month", "Day", "Day of year", "Hour", "Minute", "Seconds",
                      "Date Logger temperature", "Battery voltage", "Air temperature", "Sea Level Pressure",
                      "Raw under water sounder distance", "UW sounder distance", "Raw snow sounder distance",
                      "Snow sounder quality", "Corrected Snow sounder distance"]

# Headers for Therm data
THERM_HEADERS = ["Battery Voltage"]

# Template for Therm data headers.
THERMISTOR_TEMPERATURE_HEADERS = "Thermistor Temperature-"


WORKING_DIRECTORY = None
if len(sys.argv) < 2:
    print("Incomplete Arguments... at lease one argument required")
    exit(-1)
else:
    WORKING_DIRECTORY = sys.argv[1]

OUTPUTS_FOLDER = "Outputs"  # outputs folder if files are located there.

NO_CARRIER = "NO CARRIER"
CONNECT = "CONNECT"
RING = "RING"
END_TRANSMIT = "END TRANSMIT"

# Constants needed for successful parsing of data
DASHES = "--------------------------------------------"
GPS_TABLE = "------------- Parsed GPS Table -------------"
OUTPUT_TABLE = "------------- Output1 Table ----------------"
THERM_TABLE = "------------- Therm Table ------------------"

# Constants needed for metadata.
RINGS = "RINGS"
CONNECTION_STRING = "CONNECTION STRING"
IMB_ID = "IMB ID"
TRANSMISSION_FINISHED_SUCCESSFULLY = "TRANSMISSION FINISHED SUCCESSFULLY"


def second_imb_process(directory, year):
    files_to_process = os.listdir(directory)
    print("{} file(s) will be processed.".format(len(files_to_process)))
    if year in DIRECTORY_TREES["01"]:
        if not(year==2009):
            for curr_file in files_to_process:
                return_data = second_process_for_first_folder(curr_file, directory)
                output_file_fp = open(return_data["filename"], "w")
                output_file_fp.write(return_data["data"])
                output_file_fp.close()

    return


def second_process_for_first_folder(current_file, directory):
    """This function will handle the processing of log files generated in 2009 and 2010 (regarded as '01' files).
       -> 'current_file' is the name of the file being processed
       -> 'directory' is the path to the directory which contains the file being processed.
       -> It will return a 'csv' string representation of the processed data."""
    ring_count = 0  # how many rings occurred before transmission of data.
    connect_string = ""  # The string that indicated the establishment of a connection.
    imb_id = ""  # The id of the Buoy.

    # Strings to store the data from the different tables.
    gps_data_table = ""
    output_one_data_table = ""
    therm_data_table = ""

    # Did the transmission finish or was it broken abruptly.
    transmission_completed = True

    # Make the full path of the file to be processed and read it in as bytes.
    file_path = str(pathlib2.Path(directory, current_file))
    file_pointer = open(file_path, 'rb')

    # Read in the file line by line.
    curr_line = file_pointer.readline()

    # Get the connection string and count the number of rings.
    while curr_line and (CONNECT not in curr_line):
        if (curr_line.strip()).lower() == RING.lower():
            ring_count += 1
        curr_line = file_pointer.readline()
    connect_string = curr_line.strip()
    curr_line = file_pointer.readline()

    # Find the Buoy id.
    while curr_line and ((DASHES in curr_line) or not (curr_line.strip())):
        curr_line = file_pointer.readline()
    imb_id = curr_line.strip()

    # Find the start of the GPS table and load all the data from it into a string, and remove excess newline characters.
    while curr_line and (GPS_TABLE not in curr_line):
        curr_line = file_pointer.readline()
    curr_line = file_pointer.readline()
    while curr_line and (OUTPUT_TABLE not in curr_line):
        if curr_line.strip():
            gps_data_table += (curr_line.strip()) + "\n"  # Deals with excess new line characters.
        curr_line = file_pointer.readline()
    curr_line = file_pointer.readline()

    # When the Output table marker is found, begin loading that data into its own string until the start of the
    # Therm table.
    while curr_line and (THERM_TABLE not in curr_line):
        if curr_line.strip():
            output_one_data_table += (curr_line.strip()) + "\n"  # Deals with excess new line characters.
        curr_line = file_pointer.readline()
    curr_line = file_pointer.readline()

    # Look for an indication that the end of the transmission has been reached.
    # Either a bunch of dashes text saying the transmission is finished.
    while curr_line and (DASHES not in curr_line) and (END_TRANSMIT not in curr_line):
        if curr_line.strip():
            therm_data_table += (curr_line.strip()) + "\n"  # Deals with excess new line characters.
        curr_line = file_pointer.readline()
    if not ((DASHES in curr_line) or (END_TRANSMIT in curr_line)):
        transmission_completed = False

    # Make the string for the metadata that will be at the top of the output file.
    top_metadata = RINGS + "," + str(ring_count) + "\n" + \
                   CONNECTION_STRING + "," + connect_string + "\n" + \
                   IMB_ID + "," + imb_id + "\n" + \
                   TRANSMISSION_FINISHED_SUCCESSFULLY + "," + str(transmission_completed) + "\n" +\
                   "\n"

    # Read the gps data and the output1 data int data frames from the string,
    #  and use the datetime column as the index. Assign the appropriate headers as well using the variables at the top.
    gps_df = pd.read_csv(StringIO.StringIO(gps_data_table), header=None, index_col=0)
    gps_df.columns = GPS_HEADERS
    output_one_df = pd.read_csv(StringIO.StringIO(output_one_data_table), header=None, index_col=0)
    output_one_df.columns = OUTPUT_ONE_HEADERS

    # Read the therm table data and index with the datetime column as well.
    therm_df = pd.read_csv(StringIO.StringIO(therm_data_table), header=None, index_col=0)
    max_columns_therm = 0

    # This algorithm determines the number of headers to add to the header list for therm data and
    # adds them to the data frame
    for i in range(0, len(therm_df)):
        curr_num_columns = len(therm_df.iloc[i])
        if curr_num_columns > max_columns_therm:
            max_columns_therm = curr_num_columns
            print(len(therm_df.iloc[i]))
    headers_to_use = [] + THERM_HEADERS
    for i in range(1, (max_columns_therm - len(THERM_HEADERS)) + 1):
        headers_to_use.append(THERMISTOR_TEMPERATURE_HEADERS + str(i))
    therm_df.columns = headers_to_use

    # Do an inner join on all the data using the Datetime column as the index.
    first_merge_df = pd.merge(gps_df, output_one_df, left_index=True, right_index=True)
    final_merge_df = pd.merge(first_merge_df, therm_df, left_index=True, right_index=True)

    # Add the column header for the Datetime, make a csv string, and add the metadata to the top of the string.
    # Return this csv string, also return the name to be used for the generated file.
    final_merge_df.index.names = ['Device_DateTime_UTC']
    output_data_string = final_merge_df.to_csv()
    output_data_string = top_metadata + output_data_string

    name_to_use = str(pathlib2.Path(directory, imb_id+"-"+current_file.split('.')[0]+".csv"))

    return {"filename": name_to_use, "data": output_data_string}


def do_process(working_directory=WORKING_DIRECTORY):
    for key in DIRECTORY_TREES:
        for year in DIRECTORY_TREES[key]:
            current_dictionary = str(pathlib2.Path(working_directory, key, str(year), OUTPUTS_FOLDER))
            list_of_directories = os.listdir(current_dictionary)
            for directory in list_of_directories:
                full_dir_path = str(pathlib2.Path(current_dictionary, directory))
                second_imb_process(full_dir_path, year)

    return


#second_imb_process("C:\Users\CEOS\PycharmProjects\IMB-Scripts\\test-IMB_Data_Backup\Outputs\IMB_03272010", 2010)

#process_imb_files()

do_process()
