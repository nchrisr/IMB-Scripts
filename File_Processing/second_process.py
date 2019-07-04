import sys
import os
import pathlib2
import StringIO
import re
import pandas as pd
import traceback
import csv
import math

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

# Headers for second data type
SECOND_DATA_TYPE_INTERIM_HEADERS = ["Device_Datetime_UTC", "GPS_STRING"]+OUTPUT_ONE_HEADERS+THERM_HEADERS
SECOND_DATA_TYPE_HEADERS = []+GPS_HEADERS[0:-3]+OUTPUT_ONE_HEADERS+THERM_HEADERS


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

# Constants needed for successful parsing of data for first file format.
DASHES = "--------------------------------------------"
BAD_GPS_DATA_LINE = "gps_data="
GPS_TABLE = "------------- Parsed GPS Table -------------"
OUTPUT_TABLE = "------------- Output1 Table ----------------"
THERM_TABLE = "------------- Therm Table ------------------"
LOG_EXTENSION = ".log"

# Constants needed for successful parsing of data for second file format.
IMB_data_table = "----------------IMB_data--------------------"
GPGGA = '"$GPGGA,' # GPGGA field in gps string.

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
    print("\n{} file(s) will be processed.\n".format(len(files_to_process)))

    # Decide how to process the files based on what directory they are in and what year they are from.
    # The files need to be organized by year prior for this to work.
    if year in DIRECTORY_TREES["01"]:
        # For every file in the directory, process only the .log files, by calling the appropriate function.
        for curr_file in files_to_process:
            if curr_file.endswith(LOG_EXTENSION):
                try:
                    return_data = second_process_for_first_file_type(curr_file, directory)
                    # If None was returned, this means that the file was an empty file
                    if return_data is not None:
                        # If the file contains only metadata, state that in the .log file
                        # Write the data into the appropriate .csv file.
                        if return_data["metadata_only"]:
                            print("{} contains only metadata\n".format(return_data["filename"]))
                            errors_fp.write("{} contains only metadata \n\n".format(return_data["filename"]))
                        else:
                            print("\nSuccessfully processed file {}\n".format(curr_file))
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
                    traceback.print_exc()
                    errors_fp.write("Processing file {} failed.\n".format(curr_file))
                    errors_fp.write("\t"+str(e)+"\n\n")

    elif year in DIRECTORY_TREES["02"]:
        for curr_file in files_to_process:
            if curr_file.endswith(LOG_EXTENSION):
                try:
                    return_data = second_process_for_second_folder(curr_file, directory)
                    # If None was returned, this means that the file was an empty file
                    if return_data is not None:
                        # If the file contains only metadata, state that in the .log file
                        # Write the data into the appropriate .csv file.
                        if return_data["metadata_only"]:
                            print("{} contains only metadata\n".format(return_data["filename"]))
                            errors_fp.write("{} contains only metadata \n\n".format(return_data["filename"]))
                        else:
                            print("\nSuccessfully processed file {}\n".format(curr_file))
                        output_file_fp = open(return_data["filename"], "wb")
                        output_file_fp.write(return_data["data"])
                        output_file_fp.close()
                    else:
                        print("{} was an empty file...".format(curr_file))
                        errors_fp.write("{} was an empty file...\n\n".format(curr_file))
                except Exception as e:
                    print("Processing file {} failed.\n".format(curr_file))
                    print("\t"+str(e)+"\n")
                    traceback.print_exc()
                    errors_fp.write("Processing file {} failed.\n".format(curr_file))
                    errors_fp.write("\t"+str(e)+"\n\n")
    errors_fp.close()

    return


def second_process_for_first_file_type(current_file, directory):
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
    while curr_line and (((DASHES in curr_line) or not(curr_line.strip())) or (BAD_GPS_DATA_LINE in curr_line.lower())):
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
        # Either a bunch of dashes or text saying the transmission is finished.
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

        # Check if the gps data contains invalid characters.
        try:
            temp = gps_data_table.decode('ascii')
        except Exception as e:
            raise Exception("NON ascii characters detected in the file being processed.\n\t"+str(e))

        # Read the csv string for the gps data into a dataframe.
        gps_df = pd.read_csv(StringIO.StringIO(gps_data_table), header=None, index_col=0)

        # Check if the output-1 data contains invalid characters
        try:
            temp = output_one_data_table.decode('ascii')
        except Exception as e:
            raise Exception("NON ascii characters detected in the file being processed.\n\t"+str(e))

        # Read in output1 data into data frame from a string, index with the datetime column and assign headers.
        output_one_df = pd.read_csv(StringIO.StringIO(output_one_data_table), header=None, index_col=0)
        output_one_df.columns = OUTPUT_ONE_HEADERS

        # Check if the temperature data contains invalid characters
        try:
            temp = therm_data_table.decode('ascii')
        except Exception as e:
            raise Exception("NON ascii characters detected in the file being processed.\n\t"+str(e))

        # Read the therm table data and index with the datetime column as well.
        therm_df = pd.read_csv(StringIO.StringIO(therm_data_table), header=None, index_col=0)

        # All the data should have the same number of rows if not there is an issue.
        if not((len(gps_df) == len(therm_df)) and (len(therm_df) == len(output_one_df))):
            raise Exception("Sections of the file have different number of row(s), "
                            "this file should be processed by hand.")

        # This algorithm determines the number of headers to add to the header list for therm data and
        # adds them to the data frame
        max_columns_therm = 0
        for i in range(0, len(therm_df)):
            curr_num_columns = len(therm_df.iloc[i])
            if curr_num_columns > max_columns_therm:
                max_columns_therm = curr_num_columns
        headers_to_use = [] + THERM_HEADERS
        for i in range(1, (max_columns_therm - len(THERM_HEADERS)) + 1):
            headers_to_use.append(THERMISTOR_TEMPERATURE_HEADERS + str(i))
        therm_df.columns = headers_to_use

        # Process the gps data so that the data moves into the right columns.
        # Use the datetime column as the index. Assign the appropriate headers as well using the variables at the top.
        # Pick out the rows where the checksum value is not present.
        rows_to_shift = gps_df[gps_df[15].isnull()].index

        # Make them all strings to avoid pandas bug.
        gps_df_as_strings = gps_df.astype(str)
        num_bad_rows = len(rows_to_shift)
        count = 0
        shift_count = 0 # Number of times shift has occurred.
        # The checksum field can be assumed to be present for all rows, then shift the data until there is data in the
        # checksum field for all rows.
        while (num_bad_rows > 0) and (shift_count <= 16):
            # If you have shifted more than 16 times, the checksum values probably do not exist.
            # shift the data, get a csv string and re-read that into a dataframe to maintain the previous datatypes.
            gps_df_as_strings.loc[rows_to_shift] = gps_df_as_strings.loc[rows_to_shift].shift(periods=1, axis=1)
            gps_df_new_string = gps_df_as_strings.to_csv(header=None)
            gps_df = pd.read_csv(StringIO.StringIO(gps_df_new_string), header=None, index_col=0)

            # Get the new set of data that needs their data shifted
            rows_to_shift = gps_df[gps_df[15].isnull()].index
            num_bad_rows = len(rows_to_shift)
            count += 1
            shift_count += 1
        # Assign the column names after processing.
        gps_df.columns = GPS_HEADERS

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


def second_process_for_second_folder(current_file, directory):
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
    data_table = ""

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
    while curr_line and (((DASHES in curr_line) or not(curr_line.strip())) or (BAD_GPS_DATA_LINE in curr_line.lower())):
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

        # Find the start of the IMB_data table and load all the data from it into a string,
        # and remove excess newline characters.
        while curr_line and (IMB_data_table not in curr_line):
            curr_line = file_pointer.readline()
        curr_line = file_pointer.readline()

        # Look for an indication that the end of the transmission has been reached.
        # Either a bunch of dashes or text saying the transmission is finished.
        while curr_line and (curr_line.replace(DASHES, '')) and (END_TRANSMIT not in curr_line):
            # Clean out dashes that were transmitted on the same line as actual data
            if DASHES in curr_line:
                curr_line = curr_line.replace(DASHES, '')
            # If the line is not an empty line, then use regex to find the data
            if curr_line.strip():
                # split the line of data into a list using dates as a delimeter
                line_list = re.split('(\d+-\d+-\d+ \d+:\d+:\d+")', curr_line)
                data = '"'
                data_set_count = 0
                # If the data in a list is not a date value then parse it to handle bad quotation marks.
                for index in range(1, len(line_list)):
                    if (not(bool(re.search("(\d+-\d+-\d+ \d+:\d+:\d+)", line_list[index])))):

                        # Look for data lines* that have no dates but have GPS strings and remove them.
                        if line_list[index].count(GPGGA) > 1:
                            temp_split = re.split('("\$GPGGA,)', line_list[index])#line_list[index].split('$GPGGA')
                            print(len(temp_split[4]))
                            shortest_string=temp_split[2]
                            for location in range(1, len(temp_split)):
                                if not(temp_split[location] == GPGGA):
                                    if len(temp_split[location])<len(shortest_string):
                                        shortest_string = temp_split[location]
                            line_list[index]=line_list[index].replace(GPGGA+shortest_string, '')

                        # Remove commas from the end of the string.
                        if line_list[index][-1] == ',':
                            line_list[index] = line_list[index].rstrip(',')

                        # Count the number of quotation marks in the string
                        valid_quotes = 0
                        for character in line_list[index]:
                            if character == '"':
                                valid_quotes += 1
                        # If it is not an even number that means one of the quotation marks was not closed.
                        if valid_quotes % 2 != 0:
                            # If the last character is a quotation mark remove it, if not add a quote at the end.
                            if line_list[index][-1] == '"':
                                line_list[index] = line_list[index][0:-1]
                            else:
                                line_list[index] = line_list[index]+'"'
                    # Remove commas from the end of the string.
                    if line_list[index][-1] == ',':
                        line_list[index] = line_list[index].rstrip(',')
                    # Add the processed strings to the data variable
                    # After two strings from the list are processed that's considered one line of data.
                    data += (line_list[index]).strip()
                    data_set_count += 1
                    if data_set_count >= 2:
                        data_set_count = 0
                        data_table += data + "\n"  # Deals with excess new line characters.
                        data = '"'

            curr_line = file_pointer.readline()
        # Was the transmission ended properly?
        if not ((DASHES in curr_line) or (END_TRANSMIT in curr_line)):
            transmission_completed = False

        # Make the string for the metadata that will be at the top of the output file.
        top_metadata = RINGS + "," + str(ring_count) + "\n" + \
                       CONNECTION_STRING + "," + connect_string + "\n" + \
                       IMB_ID + "," + imb_id + "\n" + \
                       TRANSMISSION_FINISHED_SUCCESSFULLY + "," + str(transmission_completed) + "\n" + \
                       DATA_LINE + "," + str(data_line_after_connection) + "\n" + "\n"

        # Get the maximum number of fields that are in any of the rows
        rows = csv.reader(StringIO.StringIO(data_table))
        rows = list(rows)
        max_length = max(len(row) for row in rows)

        # Create the list of headers to be used for the file.
        headers_to_use = [] + SECOND_DATA_TYPE_INTERIM_HEADERS
        for i in range(1, (max_length - len(SECOND_DATA_TYPE_INTERIM_HEADERS)) + 1):
            headers_to_use.append(THERMISTOR_TEMPERATURE_HEADERS + str(i))

        # Read the data into a dataframe from the parsed string.
        full_dataframe = pd.read_csv(StringIO.StringIO(data_table), header=None, names=headers_to_use, index_col=0)

        # Remove all rows of data where the GPS string was not transmitted completely,
        # This is done because it would cause bad data in the temperature fields.
        row_count = len(full_dataframe)
        drop_indexes = []
        # for every row, check to see if the columns after the GPS string contain data
        # If the row does not, add its index to the list of indexes to be dropped.
        for row in range(0, row_count):
            columns_after_gps = full_dataframe.iloc[[row],1:]
            test = columns_after_gps.isnull().all().all()
            if test:
                the_index = columns_after_gps.index
                drop_indexes.append(the_index)

        # Delete all rows that were missing data in the temperature columns.
        for current_index in drop_indexes:
            full_dataframe = full_dataframe.drop(current_index)

        # Break up the gps string into seperate columns and delete the original gps string column
        gps_fields_df = full_dataframe["GPS_STRING"].str.split(",", expand=True)
        gps_fields_df.columns = GPS_HEADERS[0:-3]
        full_dataframe.drop("GPS_STRING", axis=1, inplace=True)
        location = 0
        for column in gps_fields_df:
            full_dataframe.insert(location, column, gps_fields_df[column])
            location += 1

        max_size = row_size(full_dataframe)
        # Remember python does not include the end index when it indexes.
        # max_size+1 is not used here because the first column is an index column and will not be included in the indexing.
        full_dataframe = full_dataframe.iloc[:,0:max_size]

        full_dataframe_string = full_dataframe.to_csv()
        output_data_string = top_metadata + full_dataframe_string

        name_to_use = str(pathlib2.Path(directory, imb_id + "-" + current_file.split('.')[0] + ".csv"))
        return {"filename": name_to_use, "data": output_data_string, "metadata_only": False}


def row_size(dataframe):
    """This function determines what is most likely the maximum number of columns that should be in 'dataframe'.
       It checks to see how many columns each of the row has,
       and determines the most frequently occurring number of columns
       [It assumes that if most of the rows have a certain number of columns,
        then that is probably the maximum number of columns that should be in the dataframe.]"""

    row_lengths = []
    # Go through each row in the dataframe and determine the number of columns in it
    for index, row in dataframe.iterrows():
        num_fields = 0
        temp_num_fields = 0
        last_loc_found = False
        # Go through each field in the row
        for value in row:
            nan_found = False
            try:
                nan_found = math.isnan(value)
            except Exception:
                # Dummy code to pass try catch block
                x=1

            if nan_found:
                last_loc_found = True
            else:
                if last_loc_found:
                    last_loc_found = False
            if last_loc_found:
                temp_num_fields += 1
            else:
                if temp_num_fields > 0:
                    num_fields += temp_num_fields
                    temp_num_fields = 0
                num_fields += 1
        row_lengths.append(num_fields)

    return most_frequent(row_lengths)


def most_frequent(my_list):
    """This function determines the which element in 'my_list' occurs most frequently."""
    counter = 0
    num = my_list[0]

    for i in my_list:
        curr_frequency = my_list.count(i)
        if (curr_frequency > counter):
            counter = curr_frequency
            num = i

    return num


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
                    second_imb_process(full_dir_path, year)
                else:
                    print("Invalid directory path {}".format(full_dir_path))

    return

# The second_imb_process function can be called on its own to process a specific set of files for a specific year.
# second_imb_process("C:\Users\CEOS\PycharmProjects\IMB-Scripts\\test-IMB_Data_Backup\Outputs\IMB_03272010", 2010)


# do_process()

# second_imb_process("C:\Users\CEOS\Desktop\Outputs\IMB_07112010", 2010)

second_imb_process("/Users/kikanye/PycharmProjects/IMB-Scripts/test_files/sample second folder process tests/IMB_02272011", 2011)
second_imb_process("/Users/kikanye/PycharmProjects/IMB-Scripts/test_files/sample second folder process tests/IMB_02282011", 2011)
second_imb_process("/Users/kikanye/PycharmProjects/IMB-Scripts/test_files/sample second folder process tests/IMB_03012011", 2011)
second_imb_process("/Users/kikanye/PycharmProjects/IMB-Scripts/test_files/sample second folder process tests/IMB_03022011", 2011)
second_imb_process("/Users/kikanye/PycharmProjects/IMB-Scripts/test_files/sample second folder process tests/IMB_03032011", 2011)
second_imb_process("/Users/kikanye/PycharmProjects/IMB-Scripts/test_files/sample second folder process tests/IMB_03042011", 2011)

print("End of processing.")
