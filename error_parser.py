import sys
import pathlib2
import pandas

ERRORS_FILE = None
if len(sys.argv) < 2:
    print("Incomplete Arguments... at lease one argument required")
    exit(-1)
else:
    ERRORS_FILE = sys.argv[1]

FAILED_OUTPUT_FILE = "Failed_errors_table.csv"
EMPTY_OUTPUT_FILE = "Empty_errors_table.csv"
METADATA_OUTPUT_FILE = "Metadata_errors_table.csv"


def parse_error_file(file_path):
    parent_directory = (pathlib2.Path(file_path)).parent
    failed_output_file = str(pathlib2.Path(parent_directory, FAILED_OUTPUT_FILE))
    empty_output_file = str(pathlib2.Path(parent_directory, EMPTY_OUTPUT_FILE))
    metadata_output_file = str(pathlib2.Path(parent_directory, METADATA_OUTPUT_FILE))

    fp = open(file_path)
    failed_errors_list = []
    empty_errors_list = []
    metadata_errors_list = []
    curr_line = fp.readline()
    while curr_line:
        if curr_line.strip():
            curr_dictionary = {"file_name":"", "error":"", "error_description":""}
            line_split = curr_line.split()
            file_name = line_split[0].strip()
            curr_dictionary["file_name"] = file_name
            error = line_split[-1].strip()
            curr_dictionary["error"] = error
            if "processing_file_failed" in error.lower():
                error_description = (fp.readline()).strip()
                curr_dictionary["error_description"] = error_description
                failed_errors_list.append(curr_dictionary)
            elif "empty_file" in error.lower():
                empty_errors_list.append(curr_dictionary)
            elif "contains_only_metadata" in error.lower():
                metadata_errors_list.append(curr_dictionary)

        curr_line = fp.readline()

    failed_errors_df = pandas.DataFrame(failed_errors_list)
    failed_errors_df.to_csv(failed_output_file)

    empty_errors_df = pandas.DataFrame(empty_errors_list)
    empty_errors_df.to_csv(empty_output_file)

    metadata_errors_df = pandas.DataFrame(metadata_errors_list)
    metadata_errors_df.to_csv(metadata_output_file)

    return


print(parse_error_file(ERRORS_FILE))

print("End of Processing...")

