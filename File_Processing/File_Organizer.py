"""The functions in this script are used to organize the raw '.log' files by year and by file type."""
import os
import shutil
import pathlib2
import sys

# Get the directory to organize from the command line.
WORKING_DIRECTORY = None
if len(sys.argv) < 2:
    print("Incomplete Arguments... at least one argument required")
    exit(-1)
else:
    WORKING_DIRECTORY = sys.argv[1]

LOG_FILE_EXTENSION = ".log"
"""The dictionary below is used to represent the directory structure which the files will be sorted into.
   The number keys point to lists that represent the year the data was collected.
   For each key, the files from the years in its list have similar structure, hence they are grouped together."""
DIRECTORY_TREES = {"01": [2009, 2010], "02": [2011, 2012, 2013], "03": [2014, 2015, 2016]}


def make_directories(working_directory=WORKING_DIRECTORY, directories=DIRECTORY_TREES):
    """This function will create directories based on the description by the 'directories' argument.
       Each key in the directories argument represents a directory (folder), and the items in its list
       represent sub-directories (sub-folders).

       The 'working_directory' argument represents the directory in which these new directories will be made."""

    # Make a directory within the directory provided at command line and name each directory made after a key from the
    # directories argument
    for curr_key in directories:
        sub_dirs = directories[curr_key]
        top_dir_name = str(pathlib2.Path(working_directory, curr_key))
        # try to make the directory and catch exceptions
        try:
            os.mkdir(top_dir_name)
            print("Successfully created directory {}".format(top_dir_name))
        except Exception as e:
            print("Making directory {} failed".format(top_dir_name))
            print(e)
        # When you make a directory for each of the keys, make a directory within them, each one named after each item
        #  in the list which they point to.
        for dir_name in sub_dirs:
            sub_dir_name = str(pathlib2.Path(top_dir_name, str(dir_name)))
            try:
                os.mkdir(sub_dir_name)
                print("Successfully created directory {}".format(sub_dir_name))
            except Exception as e:
                print("Making directory {} failed".format(sub_dir_name))
                print(e)
    return


def arrange_files(working_directory=WORKING_DIRECTORY, directories=DIRECTORY_TREES):
    """This function will arrange directories based on the description by the 'directories' argument.
       It will move the files from their current location which is specified by 'working_directory'
        into their appropriate destination based on the filename.

        The year in which the file was generated is represented by the last for characters of the filename before the
        file extension in this case."""
    files = os.listdir(working_directory)

    # move files based on the year which the data in them was collected.
    for curr_file in files:
        # Only process '.log' files
        if curr_file.endswith(LOG_FILE_EXTENSION):
            for curr_key in directories:
                sub_dirs = directories[curr_key]
                for curr_dir in sub_dirs:
                    dir_string = str(curr_dir)
                    # check if the file ends with 'current_year.log'
                    if curr_file.endswith(dir_string + LOG_FILE_EXTENSION):
                        destination = str(pathlib2.Path(working_directory, curr_key, dir_string))
                        source = str(pathlib2.Path(working_directory, curr_file))
                        try:
                            # move the file and catch errors
                            shutil.move(source, destination)
                            print("Successfully moved file {} to {}".format(curr_file, destination))
                        except Exception as e:
                            print(" Failure moving file {} to {} ".format(curr_file, destination))
                            print(e)

    return


if __name__ == "__main__":
    make_directories()
    arrange_files()
