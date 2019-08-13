"""This script will break up each IMB (.log) file in the WORKING DIRECTORY
 into multiple files using 'NO CARRIER' as a delimiter.
"""
import sys
import os
import pathlib2
import shutil

# Get the command line argument that specifies the directory to work on.
WORKING_DIRECTORY = None
if len(sys.argv) < 2:
    print("Incomplete Arguments... at lease one argument required")
    exit(-1)
else:
    WORKING_DIRECTORY = sys.argv[1]

# Strings constants needed to function
NO_CARRIER = "NO CARRIER"  # Use this to break up files
OUTPUTS_FOLDER = "Outputs"  # The name of the directory where new files are expected to be saved

# This dictionary is a representation of the directory structure which should be generated before running this script
# on the specified working directory.
DIRECTORY_TREES = {"01": [2009, 2010], "02": [2010, 2011, 2012, 2013], "03": [2013, 2014, 2015, 2016]}
LOG_FILE_EXTENSION = ".log"  # The expected extension for the IMB files


def process_regular_imb_files(working_directory=WORKING_DIRECTORY):
    """This function will process the IMB log files in the directory 'working_directory'.
        For each file in the 'working_directory' it will break it up into multiple files using the contents of the
        NO_CARRIER variable as a delimiter.
        Each file will be named using the convention: 'original_file'-'file_count'.log.
        The files generated will be saved into a directory named after the original file.
    """
    # Get the list of files in the directory.
    files_to_process = os.listdir(working_directory)
    print("{} files will be processed.".format(len(files_to_process)))
    # Make an output directory within the working directory
    outputs_path = str(pathlib2.Path(working_directory, OUTPUTS_FOLDER))
    os.mkdir(outputs_path)

    for file_ in files_to_process:
        # only process files that have the expected extension
        if file_.endswith(LOG_FILE_EXTENSION):
            try:
                # Get the full file path for the file being processed.
                file_n = str(pathlib2.Path(working_directory, file_))
                # Get the name of the file without the extension.
                file_name_use = file_.split(".")[0]

                # Make a directory named after the file being processed.
                move_path = str(pathlib2.Path(outputs_path, file_name_use))
                os.mkdir(move_path)

                # Open the file and start reading, for each data-set extracted, put it in the variable, 'csv_data'
                f = open(file_n, 'rb')
                csv_data = f.read()

                # Split on the contents of NO_CARRIER
                parse_list = csv_data.split(NO_CARRIER)

                # For each string in the split data list, make a file for it using the naming convention
                # 'original_file'-'file_count'.log.
                # Create the file and write the data into it.
                # Move each file into a directory named after the file which was processed to generate it.
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


def do_process():
    """This function will execute the process_regular_imb_files() function for each directory in the DIRECTORY TREE """
    for current_key in DIRECTORY_TREES:
        sub_directory_list = DIRECTORY_TREES[current_key]
        for sub_dir in sub_directory_list:
            working_dir = str(pathlib2.Path(WORKING_DIRECTORY, current_key, str(sub_dir)))
            process_regular_imb_files(working_dir)
    return


if __name__ == "__main__":
    do_process()




