import sys
import os
import pathlib2
import csv


WORKING_DIRECTORY = sys.argv[1]

files = os.listdir(WORKING_DIRECTORY)
print(len(files))
count = 0
for file_ in files:
    file_n = str(pathlib2.Path(WORKING_DIRECTORY, file_))
    file_name_use = file_.split(".")[0]
    f = open(file_n, 'r')
    line = f.readline()
    ring_count = 0
    connect_line = ""
    top_data_lines = []
    check_connect = True
    check_top_barrier = True
    imb_id = ""
    csv_data = ""
    imb_count = 1
    while line:
        if check_connect:
            while ("connect" not in line.lower()):
                if (line.lower()).strip() == "ring":
                    ring_count += 1
                line = f.readline()
            connect_line = line.strip()
            check_connect = False
            line = f.readline()
        else:
            if check_top_barrier:
                while ("-----" not in line.lower() and ("no carrier" not in line.lower())):
                    top_data_lines.append(line)
                    line = f.readline()
                check_top_barrier = False
                if "no carrier" not in line.lower():
                    line = f.readline()
                imb_id = line.strip() #TODO: Should assign imb_id within the if check.
            while (line.lower()).strip() != "no carrier":
                print(line)
                csv_data += line
                line = f.readline()
            curr_f = open(imb_id +"-" + file_name_use +"-" + str(imb_count) + ".log", "w")
            curr_f.write(csv_data)
            curr_f.close()
            imb_count+=1
            check_connect = True
            check_top_barrier = True
            csv_data = ""
            line = f.readline()
