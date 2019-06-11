import sys
import os
import pathlib2
import shutil
import StringIO
import pandas as pd

# This dictionary is a representation of the directory structure which should be generated before running this script
# on the specified working directory.
DIRECTORY_TREES = {"01": [2009, 2010], "02": [2011, 2012, 2013], "03": [2014, 2015, 2016]}
GPS_HEADERS = ["Device_DateTime_UTC", "$GPGGA", "GPS_Time_hhmmss", "Latitude_degrees_decimal_minutes_ddmm.mmmm", "N/S",
               "Longitude_degrees_decimal_minutes_ddmm.mmmm", "W/E", "Quality Indicator", "Number of Satellites Used",
               "HDOP(horizontal dilution of precision)", "Antenna altitude", "altitude units (M(metres)/ F(feet))",
               "Geoidal Separation", "Geoidal Separation Units (M(metres)/ F(feet))", "Correction age",
               "Checksum", "Unknown", "Unknown", "Unknown"]

WORKING_DIRECTORY = ""#sys.argv[1]
NO_CARRIER = "NO CARRIER"
CONNECT = "CONNECT"
RING = "RING"
END_TRANSMIT = "END TRANSMIT"
DASHES = "--------------------------------------------"
OUTPUTS_FOLDER = "Outputs"
GPS_TABLE = "------------- Parsed GPS Table -------------"
OUTPUT_TABLE = "------------- Output1 Table ----------------"
THERM_TABLE = "------------- Therm Table ------------------"


def second_imb_process(directory, year):
    if year in DIRECTORY_TREES["01"]:
        files_to_process = os.listdir(directory)
        print("{} files will be processed.".format(len(files_to_process)))

        for curr_file in files_to_process:
            ring_count = 0
            connect_string = ""
            imb_id = ""
            gps_data_table = ""
            output_one_data_table = ""
            therm_data_table = ""
            transmission_completed = True
            file_path = str(pathlib2.Path(directory, curr_file))
            file_pointer = open(file_path, 'rb')
            curr_line = file_pointer.readline()
            while curr_line and (CONNECT not in curr_line):
                if (curr_line.strip()).lower() == RING.lower():
                    ring_count += 1
                curr_line = file_pointer.readline()
            connect_string = curr_line.strip()
            curr_line = file_pointer.readline()
            while curr_line and ((DASHES in curr_line) or not(curr_line.strip())):
                curr_line = file_pointer.readline()
            imb_id = curr_line.strip()
            while curr_line and (GPS_TABLE not in curr_line):
                curr_line = file_pointer.readline()
            curr_line = file_pointer.readline()
            while curr_line and (OUTPUT_TABLE not in curr_line):
                if curr_line.strip():
                    gps_data_table += (curr_line.strip())+"\n"
                curr_line = file_pointer.readline()
            curr_line = file_pointer.readline()
            while curr_line and (THERM_TABLE not in curr_line):
                if curr_line.strip():
                    output_one_data_table += (curr_line.strip())+"\n"
                curr_line = file_pointer.readline()
            curr_line = file_pointer.readline()
            while curr_line and (DASHES not in curr_line) and (END_TRANSMIT not in curr_line):
                if curr_line.strip():
                    therm_data_table += (curr_line.strip())+"\n"
                curr_line = file_pointer.readline()
            if not ((DASHES in curr_line) or (END_TRANSMIT in curr_line)):
                transmission_completed = False

            gps_df = pd.read_csv(StringIO.StringIO(gps_data_table), header=None)
            gps_df.columns = GPS_HEADERS
            gps_df.to_csv("test.csv")
            print("H")



    return



def process_imb_files(working_directory=WORKING_DIRECTORY):
    """This function will process the IMB log files in the directory 'working_directory' and extract the data from it.
       For each time a connection was successfully and data was successfully transmitted,
       the data will be put into a file with the naming convention:
         'bouy_id'-'original_file'-'file_count'.log
       This file will be saved into a directory named after the original file.
       If a connection was not established to get data, a file with bouy_id 'NO CARRIER' will be created
       This function will also create a file with name 'original file'_notes.txt that contains some metadata about the
       original file.
         """
    files_to_process = os.listdir(working_directory)
    print("{} files will be processed.".format(len(files_to_process)))
    # Make an output directory within the working directory
    outputs_path = str(pathlib2.Path(working_directory, OUTPUTS_FOLDER))
    os.mkdir(outputs_path)

    for file_ in files_to_process:
        try:
            # Get the full file path for the file being processed, and get the name of the file without the extension.
            file_n = str(pathlib2.Path(working_directory, file_))
            file_name_use = file_.split(".")[0]

            # Make a directory named after the file being processed.
            move_path = str(pathlib2.Path(outputs_path, file_name_use))
            os.mkdir(move_path)

            # Make the string for the name of the notes file, and make a string variable to hold the notes for the file
            # being processed.
            notes_file_name = file_name_use + "_notes.txt"
            notes_string = ""
            # Does the file have a 'NO CARRIER' string at the end or not?
            no_carrier_end = False

            #Variables to be used while reading the file
            ring_count = 0  # How many times does the instrument ring before a connection is established.
            connect_line = ""  # A string variable to say that a connection has been established.
            check_connect = True  # To check for a connection string or not?
            check_top_barrier = True  # To check for a bunch of dashes at the top or not?

            imb_id = ""  # The id of the bouy whose data is being captured
            imb_count = 1  # How many data files have been made so far?

            # Open the file and start reading, for each dataset extracted, put it in the variable, 'csv_data'
            f = open(file_n, 'r')
            line = f.readline()
            csv_data = ""

            # While the end of the file has not been reached,
            while line:
                # Check if we need to find a connection string
                if check_connect:
                    # If a connection string has not been found and the end fo file is not reached,
                    # count the number of times the instrument rang.
                    # If close to the end of the file,
                    # check whether a unsuccessful connection was attempted before the end of the file.
                    while (CONNECT.lower() not in line.lower()) and line:
                        if (line.lower()).strip() == RING.lower():
                            ring_count += 1
                        line = f.readline()
                        if NO_CARRIER.lower() not in line.lower():
                            no_carrier_end = True
                    if line:
                        # Add the connection string to the csv_data string.
                        connect_line = line
                        csv_data += connect_line
                        check_connect = False
                        line = f.readline()
                    else:
                        # Add a metadata sub string to the notes variable, describing how the file ended.
                        if no_carrier_end:
                            notes_string += "{} rings and a 'NO CARRIER' at the end.\n".format(ring_count)
                        else:
                            notes_string += "{} rings at the end.\n".format(ring_count)
                else:
                    # If we still need to fine the set of dashes at the top,
                    # Keep searching until the id for the buoy is found
                    if check_top_barrier:
                        while (DASHES not in line.lower()) and (NO_CARRIER.lower() not in line.lower()):
                            csv_data += line
                            line = f.readline()
                        check_top_barrier = False
                        # If it is a not a 'NO_CARRIER' string, skip it and then capture the id,
                        # If not just capture the id so that it uses the 'NO_CARRIER' string to id the file'
                        if NO_CARRIER.lower() not in line.lower():
                            line = f.readline()
                        imb_id = line.strip()
                    # Read in all the lines of data for that bouy id. and add them to the string containing actual csv_data.
                    while (line.lower()).strip() != NO_CARRIER.lower():
                        csv_data += line
                        line = f.readline()

                    # Add the number of rings to the data for the file
                    # Create the file and write the data into it.
                    # Move the file into a directory named after the file which was processed to generate it.
                    csv_data = "{} RING(S)\n".format(ring_count) + csv_data
                    created_file_name = imb_id + "-" + file_name_use + "-" + str(imb_count) + ".log"
                    curr_f = open(created_file_name, "w")
                    curr_f.write(csv_data)
                    curr_f.close()
                    shutil.move(created_file_name, move_path)

                    # Reset housekeeping variables
                    # Go the next line.
                    ring_count = 0
                    imb_count += 1
                    check_connect = True
                    check_top_barrier = True
                    csv_data = ""
                    line = f.readline()

            # Write the metadata into a text file, close it and move it into a directory named after the original file.
            notes_f = open(notes_file_name, "w")
            notes_f.write(notes_string)
            notes_f.close()
            shutil.move(notes_file_name, move_path)
            print("Successfully processed {}".format(file_))
        except Exception as e:
            print(e)
            print "Processing {} failed".format(file_)

    return

second_imb_process("C:\Users\CEOS\PycharmProjects\IMB-Scripts\\test-IMB_Data_Backup\Outputs\IMB_03272010", 2010)

#process_imb_files()