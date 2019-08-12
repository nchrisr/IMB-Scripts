This script will move files based into directories that signify their file type, based on the year which the data was
collected. (For a more elaborate description on the file types, see **Raw_files.md**).

The function `make_directories(working_directory=WORKING_DIRECTORY, directories=DIRECTORY_TREE)` makes all a directory 
tree described by the argument `directories`. The default value for this argument is the global variable `DIRECTORY_TREE`.
The keys in this variable represent a top level directory. Each of these keys have a value which is a list,
 and the items in this list represent directories to be made within the directory named after that key.
  > As an example if the dictionary has a key value pair `"01": [2009, 2010]` a directory structure will be created 
  like in the diagram below:
  
  >      |------01
  >              |-------2009
  >              |-------2010
 

The function `arrange_files(working_directory=WORKING_DIRECTORY, directories=DIRECTORY_TREES)` will move all the files
which have the extension **".log"** from the directory which was provided as a command line argument into one of the
directories created. It will do this using the last four characters of the file's name. 
If the last four characters of the file's name are the same as one of the lowest level directories,
the file will be moved there.
