import json
import pathlib2

CONTENT = "CONTENT"
NAME = "NAME"
NAME_AND_CONTENTS = "NAME AND CONTENTS"

EQUALITY_OF_CONTENTS = "equality_by_contents.json"
EQUALITY_OF_NAME = "equality_by_name.json"
EQUALITY_OF_NAME_AND_CONTENTS = "equality_by_name_and_contents.json"


def read_jsons(json_content, type):
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
    with open(EQUALITY_OF_CONTENTS, 'r') as json_contents:
        equality_of_contents_data = json.load(json_contents)
    read_jsons(equality_of_contents_data, CONTENT)

    with open(EQUALITY_OF_NAME, 'r') as json_contents:
        equality_of_name_data = json.load(json_contents)
    read_jsons(equality_of_name_data, NAME)

    with open(EQUALITY_OF_NAME_AND_CONTENTS, 'r') as json_contents:
        equality_of_name_and_contents_data = json.load(json_contents)
    read_jsons(equality_of_name_and_contents_data, NAME_AND_CONTENTS)


    print("\nNumber of files with same contents: {}\nNumber of files with same names: {}\n"
          "Number of files with same name and contents: {}."
          .format(len(equality_of_contents_data), len(equality_of_name_data),
                  len(equality_of_name_and_contents_data)))

    return


if __name__ == '__main__':
    main()
    print("End of processing...")
