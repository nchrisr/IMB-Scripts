"""This script will read the json files which describe the duplicates of the raw files, and print the details."""

import json
import pathlib2

# Constants for the names of json files and the types of their content.
CONTENT = "CONTENT"
NAME = "NAME"
NAME_AND_CONTENTS = "NAME AND CONTENTS"

EQUALITY_OF_CONTENTS = "equality_by_contents.json"
EQUALITY_OF_NAME = "equality_by_name.json"
EQUALITY_OF_NAME_AND_CONTENTS = "equality_by_name_and_contents.json"


def read_jsons(json_content, type):
    """process the json_content based on the type argument. Print the details. """
    if type == CONTENT:
        for item in json_content:
            directory_1 = pathlib2.Path(json_content[item][0])
            directory_2 = pathlib2.Path(json_content[item][1])

            print("The file '{}' in the '{}' folder has the same contents as '{}' in the '{}' folder.".
                  format(item, directory_1.name, item, directory_2.name))
    elif type == NAME:
        for item in json_content:
            directory_1 = pathlib2.Path(json_content[item][0])
            directory_2 = pathlib2.Path(json_content[item][1])

            print("The file '{}' in the '{}' has the same name as '{}' in the '{}' folder, so they might be the same".
                  format(item, directory_1.name, item, directory_2.name))
    elif type == NAME_AND_CONTENTS:
        for item in json_content:
            directory_1 = pathlib2.Path(json_content[item][0])
            directory_2 = pathlib2.Path(json_content[item][1])

            print("The file '{}' in the '{}' folder has the same name and contents as '{}' in the '{}' folder. ".
                  format(item, directory_1.name, item, directory_2.name))
    return


def main():
    """Read in json files and print the details."""
    with open(EQUALITY_OF_CONTENTS, 'r') as json_contents:
        equality_of_contents_data = json.load(json_contents)
    read_jsons(equality_of_contents_data, CONTENT)

    with open(EQUALITY_OF_NAME, 'r') as json_contents:
        equality_of_name_data = json.load(json_contents)
    read_jsons(equality_of_name_data, NAME)

    with open(EQUALITY_OF_NAME_AND_CONTENTS, 'r') as json_contents:
        equality_of_name_and_contents_data = json.load(json_contents)
    read_jsons(equality_of_name_and_contents_data, NAME_AND_CONTENTS)

    # Print the number of file found to have duplicates for each kind of duplicates.
    print("\nNumber of files with same contents: {}"
          "\nNumber of files with same names: {}"
          "\nNumber of files with same name and contents: {}."
          .format(len(equality_of_contents_data), len(equality_of_name_data),
                  len(equality_of_name_and_contents_data)))

    return


if __name__ == '__main__':
    main()
    print("End of processing...")
