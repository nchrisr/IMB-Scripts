"""
This script will check specified folders and compare their contents to look for files with the same name and contents.
The folders must be specified as command line arguments.
"""
import os
import pathlib2
import json
import filecmp
import sys

ARGS_COUNT = 3
arguments = sys.argv
if len(arguments) < ARGS_COUNT:
    print("Incomplete arguments... {} arguments received, {} required ".format(len(arguments), ARGS_COUNT))

FIRST_DIRECTORY = arguments[1]
SECOND_DIRECTORY = arguments[2]

# Dictionaries to hold the information about duplicated files.
# One for duplicates based on name only, one for duplicates based on contents only, one for duplicates based on name
# and content.
equality_by_name_dict = {}
equality_by_contents_dict = {}
equality_by_name_and_contents_dict = {}


def check_for_same_files(directory_1, directory_2):
    """This function will check the directories specified as the arguments,
    for files which have same name or same contents and both (have same name and contents)."""
    directory_1_files = os.listdir(directory_1)
    directory_2_files = os.listdir(directory_2)

    # Print details of processing.
    print("{} files in {}; {} files in {}".format(len(directory_1_files), directory_1,
                                                  len(directory_2_files), directory_2))

    # For every file in the first specified arguments.
    for item_1 in directory_1_files:
        # Only process '.log' files.
        if (item_1.lower()).endswith('.log'):
            print("\nChecking for duplicates for {}".format(item_1))
            for item_2 in directory_2_files:
                if (item_1.lower()).endswith('.log'):
                    # Check for name equality
                    name_equality = item_1.lower() == item_2.lower()
                    # Check for content equality
                    content_equality = filecmp.cmp(str(pathlib2.Path(directory_1, item_1)),
                                                   str(pathlib2.Path(directory_2, item_2)))
                    if name_equality and (not(content_equality)):
                        print("{} has a duplicate name but not same contents.".format(item_1))

                    # Update the dictionaries representing the findings of the script.
                    if name_equality:
                        equality_by_name_dict[item_1] = [directory_1, directory_2]
                    if content_equality:
                        equality_by_contents_dict[item_1] = [directory_1, directory_2]
                    if name_equality and content_equality:
                        equality_by_name_and_contents_dict[item_1] = [directory_1, directory_2]
    return


def main():
    """This function calls check_for_same_files and writes the results to their respective json files. """
    check_for_same_files(FIRST_DIRECTORY, SECOND_DIRECTORY)

    with open('equality_by_name.json', 'w') as out_f:
        json.dump(equality_by_name_dict, out_f, indent=4)

    with open('equality_by_contents.json', 'w') as n_out_f:
        json.dump(equality_by_contents_dict, n_out_f, indent=4)

    with open('equality_by_name_and_contents.json', 'w') as n_out_ff:
        json.dump(equality_by_name_and_contents_dict, n_out_ff, indent=4)

    return


if __name__ == '__main__':
    main()
    print("End of processing... ")
