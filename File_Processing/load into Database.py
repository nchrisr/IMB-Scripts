import pymongo
import csv
import pandas as pd

#list_of_rows = final_merge_df.to_dict("records")
reader = csv.DictReader(StringIO.StringIO(output_data_string))
data_list = []
for row in reader:
    print row
    data_list.append(row)

# List of symbols to remove
BAD_SYMBOLS = ["-", "'", '"']
"""for current_dictionary in list_of_rows:
    for key in current_dictionary:
        if current_dictionary[key] in BAD_SYMBOLS:
            current_dictionary[key] = None
        if key == "$GPGGA":
            try:
                if current_dictionary[key]!="$GPGGA":
                    current_dictionary["$GPGGA"] = None
            except Exception as e:
                current_dictionary[key] = None
        elif key == "GPS_Time_hhmmss":
            try:
                current_dictionary[key] = int(current_dictionary[key])
                if len(((str(current_dictionary[key])).split('.'))[0]) != 5:
                    current_dictionary[key] = None
            except Exception as e:
                current_dictionary[key] = None
        elif key == "Latitude_degrees_decimal_minutes_ddmm.mmmm":
            try:
                current_dictionary[key] = float(current_dictionary[key])
            except Exception as e:
                current_dictionary[key] = None
        elif key == "N/S":
            try:
                if current_dictionary[key].lower()!="n" and current_dictionary[key].lower()!="s":
                    current_dictionary["key"] = None
            except Exception as e:
                current_dictionary[key] = None
        elif key == "Longitude_degrees_decimal_minutes_ddmm.mmmm":
            try:
                current_dictionary[key] = float(current_dictionary[key])
            except Exception as e:
                current_dictionary[key] = None
        elif key == "W/E":
            try:
                if current_dictionary[key].lower()!="w" and current_dictionary[key].lower()!="e":
                    current_dictionary["key"] = None
            except Exception as e:
                current_dictionary[key] = None
        elif key == "Quality Indicator":
            try:
                current_dictionary[key] = float(current_dictionary[key])
            except Exception as e:
                current_dictionary[key] = None
        elif key == "Number of Satellites Used":
            try:
                current_dictionary[key] = int(current_dictionary)
            except Exception as e:
                current_dictionary[key] = None
        elif key == "HDOP(horizontal dilution of precision)":
            try:
                current_dictionary[key] = float(current_dictionary[key])
            except Exception as e:
                current_dictionary[key] = None
        elif key == "Antenna altitude":
            try:
                current_dictionary[key] = float(current_dictionary[key])
            except Exception as e:
                current_dictionary[key] = None
        elif key == "altitude units (M(metres)/F(feet))":
            try:
                if current_dictionary[key].lower() != "m" and current_dictionary[key].lower() != "f":
                    current_dictionary["key"] = None
            except Exception as e:
                current_dictionary[key] = None
        elif key == "Geoidal Separation":
            try:
                current_dictionary[key] = float(current_dictionary[key])
            except Exception as e:
                current_dictionary[key] = None
        elif key == "Geoidal Separation Units (M(metres)/F(feet))":
            try:
                if current_dictionary[key].lower() != "m" and current_dictionary[key].lower() != "f":
                    current_dictionary["key"] = None
            except Exception as e:
                current_dictionary[key] = None
        elif key == "Checksum":
            try:
                if not(bool(re.search("(\*\d)(\d|[a-zA-Z])", str(current_dictionary[key])))):
                    current_dictionary[key] = None
            except Exception as e:
                current_dictionary[key] = None
        elif key == "Year":
            try:
                current_dictionary[key] = int(current_dictionary[key])
            except Exception as e:
                current_dictionary[key] = None
        elif key == "Month":
            try:
                if not(1 <= int(current_dictionary[key]) <= 12):
                    current_dictionary[key] = None
            except Exception as e:
                current_dictionary[key] = None

        elif key == "Day":
            try:
                if not (1 <= int(current_dictionary[key]) <= 31):
                    current_dictionary[key] = None
            except Exception as e:
                current_dictionary[key] = None
        elif key == "Day of Year":
            try:
                if not(1 <= int(current_dictionary[key]) <= 365):
                    current_dictionary[key] = None
            except Exception as e:
                current_dictionary[key] = None
        elif key == "Hour":
            try:
                if not(0 <= int(current_dictionary[key]) <= 23):
                    current_dictionary[key] = None
            except Exception as e:
                current_dictionary[key] = None
        elif key == "Minute":
            try:
                if not(0 <= int(current_dictionary[key]) <= 23):
                    current_dictionary[key] = None
            except Exception as e:
                current_dictionary[key] = None
        elif key == "Second":
            try:
                current_dictionary[key] = int(current_dictionary[key])
            except Exception as e:
                current_dictionary[key] = None
        elif key == "Date Logger temperature" or key == "Battery voltage" or key == "Air temperature" or key == "Sea Level Pressure":
            try:
                current_dictionary[key] = float(current_dictionary[key])
            except Exception as e:
                current_dictionary[key] = None
        elif key == "Raw under water sounder distance":
            try:
                if not(bool(re.search('^\R\d+\.?\d*$', current_dictionary[key]))):
                    current_dictionary[key] = None
            except Exception as e:
                current_dictionary[key] = None
        elif key == "UW sounder distance" or key == "Raw sounder quality" or key == "Corrected Snow sounder distance" or key == "Vattery Voltage":
            try:
                current_dictionary[key] = float(current_dictionary[key])
            except Exception as e:
                current_dictionary[key] = None

        elif THERMISTOR_TEMPERATURE_HEADERS in key:
            try:
                float(current_dictionary[key])
            except Exception as e:
                current_dictionary[key] = None"""