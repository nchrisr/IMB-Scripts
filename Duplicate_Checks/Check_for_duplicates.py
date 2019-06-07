"""This script will check spceified folders and compare their contents to look for files with the same name """
import pandas as pd
from pandas.testing import assert_frame_equal
import os
import pathlib2
import json
import filecmp
import sys

ARGS_COUNT = 3
arguments = sys.argv
if len(arguments)< ARGS_COUNT:
    print("Incomplete arguments... {} arguments received, {} required ".format(len(arguments), ARGS_COUNT))

FIRST_DIRECTORY = arguments[1] #"C:\Users\CEOS\Desktop\IMB DATA_CHECKS\ARCTICNET 2009 SEAICE\IMB"
SECOND_DIRECTORY = arguments[2] #"C:\Users\CEOS\Desktop\IMB DATA_CHECKS\IMB DATA BACKUP\IMB_Data_Backup"


equality_by_name_dict = {}
equality_by_contents_dict = {}
equality_by_name_and_content_dict = {}


def read_data(path):
    """Read in the file specified by path based on the extension of the file."""
    if path.lower().endswith('.log'):
        d_frame = pd.read_csv(path)
    else:
        print("Invalid file type provided {}".format(path.lower()))
        exit(-1)
    return d_frame


def check_dataframe_equality(df1, df2):
    """Check that the two data frames df1 and df2 are equal """
    try:
        return_val = assert_frame_equal(df1, df2, check_dtype=False)
    except Exception as e:
        print ("The Dataframes are different...")
        print(e)
        return_val = False
    if return_val is None:
        return_val = True
    return return_val


def check_for_same_files(directory_1, directory_2):
    directory_1_files = os.listdir(directory_1)
    directory_2_files = os.listdir(directory_2)

    print("{} files in {}; {} files in {}".format(len(directory_1_files), directory_1,
                                                  len(directory_2_files), directory_2))

    for item_1 in directory_1_files:
        if (item_1.lower()).endswith('.log'):
            print("\nChecking for duplicates for {}".format(item_1))
            for item_2 in directory_2_files:
                if (item_1.lower()).endswith('.log'):
                    # df_item_1 = read_data(str(pathlib2.Path(directory_1, item_1)))
                    # df_item_2 = read_data(str(pathlib2.Path(directory_2, item_2)))
                    name_equality = item_1.lower() == item_2.lower()
                    content_equality = filecmp.cmp(str(pathlib2.Path(directory_1, item_1)),
                                                   str(pathlib2.Path(directory_2, item_2)))
                    if name_equality and not(content_equality):
                        print("JH")
                    if name_equality:
                        equality_by_name_dict[item_1] = [directory_1, directory_2]
                    if content_equality:
                        equality_by_contents_dict[item_1] = [directory_1, directory_2]
                    if name_equality and content_equality:
                        equality_by_name_and_content_dict[item_1] = [directory_1, directory_2]

    return


def main():
    check_for_same_files(FIRST_DIRECTORY, SECOND_DIRECTORY)

    with open('equality_by_name.json', 'w') as out_f:
        json.dump(equality_by_name_dict, out_f, indent=4)

    with open('equality_by_contents.json', 'w') as n_out_f:
        json.dump(equality_by_contents_dict, n_out_f, indent=4)

    with open('equality_by_name_and_contents.json', 'w') as n_out_ff:
        json.dump(equality_by_name_and_content_dict, n_out_ff, indent=4)

    return


if __name__ == '__main__':
    main()
    print("End of processing... ")
