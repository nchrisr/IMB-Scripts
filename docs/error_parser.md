# **error_parser.py**

This script works by looking for certain key words/phrases in each error line and classifies the errors based on those.
It selects which file to add the error line to based on these key words/phrases:

- **"processing_file_failed"**: Which means the error will be added to the **Failed_errors_table.csv** file
- **"empty_file"**: Which means the error will be added to the **Empty_errors_table.csv** file.
- **"contains_only_metadata":** Which means the error will be added to the **Metadata_errors_table.csv** file.


It reads in every line from the error file if it finds the text **"processing_file_failed"** it will read in the next 
line which is a description of the error, otherwise it just continues.

Each error in the file is read into a dictionary with three keys: ***file_name, error and error_description***. 
In order to get the values for these three keys, the line is split on an empty space to make a list of strings.
- The first item in this list is the **file_name**
- The last item is the **error**.
- If the **error** contains the string **"processing_file_failed"**, then the line right after it will be read, 
and that will be the value for the **error_description**.
 
A list of dictionaries is created for each error type, and each of them are converted into csv files. Hence three (3) 
files will be created namely: **Failed_errors_table.csv, Empty_errors_table.csv, Metadata_errors_table.csv**.
These files will be in the same directory as the error file which was parsed.



