import sys
import os
import pathlib2
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
BAD_GPS_DATA_LINE = "gps_data="
GPS_TABLE = "------------- Parsed GPS Table -------------"
OUTPUT_TABLE = "------------- Output1 Table ----------------"
THERM_TABLE = "------------- Therm Table ------------------"
LOG_EXTENSION = ".log"

# Constants needed for metadata.
RINGS = "RINGS"
CONNECTION_STRING = "CONNECTION STRING"
IMB_ID = "IMB ID"
TRANSMISSION_FINISHED_SUCCESSFULLY = "TRANSMISSION FINISHED SUCCESSFULLY"
DATA_LINE = "DATA LINE AFTER CONNECTION STRING"

# Name of the file containing error logs.
ERRORS_FILE = "second_process_errors.txt"


def second_imb_process(directory, year):
    """
     This function will execute the second process in handling the IMB log files.
     It will process the files which by calling different functions based on what year the files are from.
     The 'directory' argument specifies the directory which the files are located in
     The 'year' argument specifies what year the data in the file is from.

     It generates an error log file that describes all the files that had issues during the process,
     and saves it in the parent directory of the 'directory' argument.

     """
    # Get the parent directory of the directory argument, and open an error log file.
    parent = str(pathlib2.Path(directory).parent)
    error_file_path = str(pathlib2.Path(parent, ERRORS_FILE))
    errors_fp = open(error_file_path, "a+")

    # Get a list of all the files in the directory and process them.
    files_to_process = os.listdir(directory)
    print("{} file(s) will be processed.\n".format(len(files_to_process)))

    # Decide how to process the files based on what directory they are in and what year they are from.
    # The files need to be organized by year prior for this to work.
    if year in DIRECTORY_TREES["01"]:
        # For every file in the directory, process only the .log files, by calling the appropriate function.
        for curr_file in files_to_process:
            if curr_file.endswith(LOG_EXTENSION):
                try:
                    return_data = second_process_for_first_folder(curr_file, directory)
                    # If None was returned, this means that the file was an empty file
                    if return_data is not None:
                        # If the file contains only metadata, state that in the .log file
                        # Write the data into the appropriate .csv file.
                        if return_data["metadata_only"]:
                            print("{} contains only metadata\n".format(return_data["filename"]))
                            errors_fp.write("{} contains only metadata \n\n".format(return_data["filename"]))
                        output_file_fp = open(return_data["filename"], "wb")
                        output_file_fp.write(return_data["data"])
                        output_file_fp.close()
                    else:
                        print("{} was an empty file...".format(curr_file))
                        errors_fp.write("{} was an empty file...\n\n".format(curr_file))
                except Exception as e:
                    # Catch errors and write them to error file and print them to screen as well.
                    print("Processing file {} failed.\n".format(curr_file))
                    print("\t"+str(e)+"\n")
                    errors_fp.write("Processing file {} failed.\n".format(curr_file))
                    errors_fp.write("\t"+str(e)+"\n\n")
    errors_fp.close()

    return


def second_process_for_first_folder(current_file, directory):
    """This function will handle the processing of log files generated in 2009 and 2010 (regarded as '01' files).

       -> 'current_file' is the name of the file being processed
       -> 'directory' is the path to the directory which contains the file being processed.
       -> It will return a dictionary with three key-value pairs.

       "filename"-> the file path where the processed data should be saved to.
       "data"-> A 'csv' string representation of the processed data.
       "metadata_only"-> A boolean value indicating whether or not the file has actual data or just metadata."""
    data_line_after_connection = None
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
    while (curr_line and ((DASHES in curr_line) or not(curr_line.strip()))) or (BAD_GPS_DATA_LINE in curr_line.lower()):
        if BAD_GPS_DATA_LINE in curr_line.lower():
            data_line_after_connection = curr_line
        curr_line = file_pointer.readline()

    # If it gets here and there was no data found, there is probably no data in the file or it contains just 'rings'.
    if not curr_line:
        print("File with no data found ... ")
        # If the file contained rings, store that. Rings are considered metadata.
        # If there were no rings, then it was probably an empty file.
        if ring_count > 0:
            transmission_completed = False
            connect_string = str(None)
            imb_id = str(None)
            top_metadata = RINGS + "," + str(ring_count) + "\n" + \
                           CONNECTION_STRING + "," + connect_string + "\n" + \
                           IMB_ID + "," + imb_id + "\n" + \
                           TRANSMISSION_FINISHED_SUCCESSFULLY + "," + str(transmission_completed) + "\n" + \
                           DATA_LINE + "," + str(data_line_after_connection) + "\n" + "\n"

            name_to_use = str(pathlib2.Path(directory, imb_id + "-" + current_file.split('.')[0] + ".csv"))
            return {"filename": name_to_use, "data": top_metadata, "metadata_only": True}
        else:
            return None
    else:
        # Store the Buoy id.
        imb_id = curr_line.strip()

        # Find the start of the GPS table and load all the data from it into a string,
        # and remove excess newline characters.
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
                       TRANSMISSION_FINISHED_SUCCESSFULLY + "," + str(transmission_completed) + "\n" + \
                       DATA_LINE + "," + str(data_line_after_connection) + "\n" + "\n"

        # Read in the csv string for the gps data and process it so that the data moves into the right columns.
        # Use the datetime column as the index. Assign the appropriate headers as well using the variables at the top.
        gps_df = pd.read_csv(StringIO.StringIO(gps_data_table), header=None, index_col=0)
        # Pick out the rows where the checksum value is not present.
        rows_to_shift = gps_df[gps_df[15].isnull()].index

        # Make them all strings to avoid pandas bug.
        gps_df_as_strings = gps_df.astype(str)
        num_bad_rows = len(rows_to_shift)
        count = 0
        # The checksum field can be assumed to be present for all rows, then shift the data until there is data in the
        # checksum field for all rows.
        while num_bad_rows > 0:
            # shift the data, get a csv string and re-read that into a dataframe to maintain the previous datatypes.
            gps_df_as_strings.loc[rows_to_shift] = gps_df_as_strings.loc[rows_to_shift].shift(periods=1, axis=1)
            gps_df_new_string = gps_df_as_strings.to_csv(header=None)
            gps_df = pd.read_csv(StringIO.StringIO(gps_df_new_string), header=None, index_col=0)

            # Get the new set of data that needs their data shifted
            rows_to_shift = gps_df[gps_df[15].isnull()].index
            num_bad_rows = len(rows_to_shift)
            count += 1
        # Assign the column names after processing.
        gps_df.columns = GPS_HEADERS

        # Read in output1 data into data frame from a string.
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
        headers_to_use = [] + THERM_HEADERS
        for i in range(1, (max_columns_therm - len(THERM_HEADERS)) + 1):
            headers_to_use.append(THERMISTOR_TEMPERATURE_HEADERS + str(i))
        therm_df.columns = headers_to_use

        if not((len(gps_df) == len(therm_df)) and (len(therm_df) == len(output_one_df))):
            raise Exception("Sections of the file have different number of row(s), "
                            "this file should be processed by hand.")

        # Do an inner join on all the data using the Datetime column as the index.
        first_merge_df = pd.merge(gps_df, output_one_df, left_index=True, right_index=True)
        final_merge_df = pd.merge(first_merge_df, therm_df, left_index=True, right_index=True)

        # Add the column header for the Datetime, make a csv string, and add the metadata to the top of the string.
        # Return this csv string, also return the name to be used for the generated file.
        final_merge_df.index.names = ['Device_DateTime_UTC']
        output_data_string = final_merge_df.to_csv()
        output_data_string = top_metadata + output_data_string

        name_to_use = str(pathlib2.Path(directory, imb_id+"-"+current_file.split('.')[0]+".csv"))

        return {"filename": name_to_use, "data": output_data_string, "metadata_only": False}


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
                second_imb_process(full_dir_path, year)

    return

# The second_imb_process function can be called on its own to process a specific set of files for a specific year.
# second_imb_process("C:\Users\CEOS\PycharmProjects\IMB-Scripts\\test-IMB_Data_Backup\Outputs\IMB_03272010", 2010)


do_process()

#second_imb_process("C:\Users\CEOS\Desktop\Outputs\IMB_07112010", 2010)

print("End of processing.")
