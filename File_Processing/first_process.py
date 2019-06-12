import sys
import os
import pathlib2
import shutil

WORKING_DIRECTORY = None
if len(sys.argv) < 2:
    print("Incomplete Arguments... at lease one argument required")
    exit(-1)
else:
    WORKING_DIRECTORY = sys.argv[1]

NO_CARRIER = "NO CARRIER"
CONNECT = "CONNECT"
RING = "RING"
DASHES = "-----------"
OUTPUTS_FOLDER = "Outputs"
# This dictionary is a representation of the directory structure which should be generated before running this script
# on the specified working directory.
DIRECTORY_TREES = {"01": [2009, 2010], "02": [2011, 2012, 2013], "03": [2014, 2015, 2016]}
LOG_FILE_EXTENSION = ".log"


def process_regular_imb_files(working_directory=WORKING_DIRECTORY):
    """This function will process the IMB log files in the directory 'working_directory' and extract the data from it.
       For each time a connection was successfully and data was successfully transmitted,
       the data will be put into a file with the naming convention:
         'original_file'-'file_count'.log
       This file will be saved into a directory named after the original file.
    """
    # Get the list of files in the directory.
    files_to_process = os.listdir(working_directory)
    print("{} files will be processed.".format(len(files_to_process)))
    # Make an output directory within the working directory
    outputs_path = str(pathlib2.Path(working_directory, OUTPUTS_FOLDER))
    os.mkdir(outputs_path)

    for file_ in files_to_process:
        if file_.endswith(LOG_FILE_EXTENSION):
            try:
                # Get the full file path for the file being processed, and get the name of the file without the extension.
                file_n = str(pathlib2.Path(working_directory, file_))
                file_name_use = file_.split(".")[0]

                # Make a directory named after the file being processed.
                move_path = str(pathlib2.Path(outputs_path, file_name_use))
                os.mkdir(move_path)

                # Open the file and start reading, for each dataset extracted, put it in the variable, 'csv_data'
                f = open(file_n, 'rb')
                csv_data = f.read()

                parse_list = csv_data.split(NO_CARRIER)

                # Add the number of rings to the data for the file
                # Create the file and write the data into it.
                # Move the file into a directory named after the file which was processed to generate it.
                for i in range(0, len(parse_list)):
                    created_file_name = file_name_use + "-" + str(i+1) + LOG_FILE_EXTENSION
                    curr_f = open(created_file_name, "w")
                    curr_f.write(parse_list[i])
                    curr_f.close()
                    shutil.move(created_file_name, move_path)
                print("Successfully processed {}".format(file_))

            except Exception as e:
                print(e)
                print "Processing {} failed".format(file_)

    return


for current_key in DIRECTORY_TREES:
    sub_directory_list = DIRECTORY_TREES[current_key]
    for sub_dir in sub_directory_list:
        working_dir = str(pathlib2.Path(WORKING_DIRECTORY, current_key, str(sub_dir)))
        process_regular_imb_files(working_dir)

