# ODV file parser



import os
import sys
import logging
import time
import re
from types import FunctionType, NoneType
import typing as ty
from scipy import stats
import pickle



# USER VARIABLES

# remove measurements from a set that have the absolute value of their z-score higher than the threshold
# set to 'float("inf)' to disable removing measurements
setting_z_score_threshold = float("Inf")

# remove measurements with the Chl measurement value the same as the previous measurement in files with a repeat coefficient of at least this much
# repeat coefficient ranges from 0 (no repeats - no two adjacent values are the same) to 1 (all repeats - one unique value)
# set to a value higher than 1 to disable removing measurements
setting_filter_repeat_coefficient_threshold = 1.1

# print out full tables for each step (visible in the log file)
setting_logger_print_table_after_each_step = False
# print out full table at the start (visible in the log file)
setting_logger_print_table_at_start = False
# print out full table at the end (visible in the log file)
setting_logger_print_table_at_end = False

# minimum quality that the files need to have in order to be processed and included in the final result
# this quality is defined in the file list with '[quality:<value>]'
# set to '0' to disable
setting_quality_min_threshold = 0

# serialize and save the data from the parsed files (before the data is processed) and load it back into memory on the next script executions
setting_serialize_and_deserialize_parsed_data = False



# PROGRAM VARIABLES

_logger = logging.getLogger("main")

_time_start = None

_column_names = []

_data_raw_serialized_filename = "data_raw_serialized.dat"

_input_filename = "filelist.txt"
_input_filename_out = "situ.csv"



# PROGRAM CONSTANTS

_column_name_map = {
    "CRUISE": "Cruise",
    "STATION": "Station",
    "TYPE": "Type",
    "YYYY-MM-DDTHH:MM:SS.SSS": "DateTime",
    "BOT. DEPTH [M]": "BotDepth",
    "LATITUDE [DEGREES_NORTH]": "Lat",
    "LONGITUDE [DEGREES_EAST]": "Lon",
    "TIME_ISO8601 [YYYY-MM-DDTHH:MM:SS.SSS]": "DateTime",
    "CPHLFLP1 [MICROGRAMS PER LITRE]": "Chl",
    "ADEPZZ01 [METERS]": "SampleDepth",
    "CHLOROPHYLL A [UG/L]": "Chl",
    "DEPTH BELOW SURFACE OF THE WATER BODY [M]": "SampleDepth",
    "SEA-FLOOR DEPTH [M]": "FloorDepth",
    "CHLOROPHYLL A [MICROG/L]": "Chl",
    "DEPTH [M]": "SampleDepth",
    "CPWC [MILLIGRAM/M3]": "Chl",
    "CHLOROPHYLL-A [UG/L]": "Chl",
    "DEPTH OF SAMPLING [M]": "SampleDepth",
    "CPHLFLP2 [MICROGRAMS PER LITRE]": "Chl",
    "ADEPZZ01 [M]": "SampleDepth",
    "CHLOROPHYLL-A [MG/M^3]": "Chl",
    "ADEPZZ01 [METRES]": "SampleDepth",
    "CPHLPL01 [MILLIGRAMS PER CUBIC METRE]": "Chl",
    "DEPBELOWSURFACE [M]": "SampleDepth",
    "CPHLPRKG [MICROGRAMS PER LITRE]": "Chl",
    "CPHLZZXX [MILLIGRAMS PER CUBIC METRE]": "Chl"
}

_column_parse_format_map = {
    "Cruise": "str",
    "Station": "str",
    "Type": "str",
    "Lon": "float",
    "Lat": "float",
    "DateTime": "datetime",
    "Chl": "float",
    "BotDepth": "float",
    "SampleDepth": "float",
    "FloorDepth": "float"
}

_column_name_required = {
    "Lon",
    "Lat",
    "DateTime",
    "Chl"
}

_column_names_out_list = [
    "file_id",
    "seq_in",
    "total_in",
    "seq_all",
    "date_time",
    "exact_datetime",
    "lon",
    "lat", 
    "chl",
    "chl_repeat_coef",
    "bot_depth",
    "floor_depth",
    "sample_depth",
    "quality",
    "file_type"
]

_column_original_name_priority_list = {
    "DateTime": [
        "YYYY-MM-DDTHH:MM:SS.SSS",
        "TIME_ISO8601 [YYYY-MM-DDTHH:MM:SS.SSS]"
        ],
    "Lon": [
        "LONGITUDE [DEGREES_EAST]"
        ],
    "Lat": [
        "LATITUDE [DEGREES_NORTH]"
        ],
    "Chl": [
        "CPHLPL01 [MILLIGRAMS PER CUBIC METRE]",
        "CHLOROPHYLL-A [MG/M^3]",
        "CPHLFLP2 [MICROGRAMS PER LITRE]",
        "CHLOROPHYLL-A [UG/L]",
        "CPWC [MILLIGRAM/M3]",
        "CHLOROPHYLL A [MICROG/L]",
        "CHLOROPHYLL A [UG/L]",
        "CPHLPL01 [MILLIGRAMS PER CUBIC METRE]",
        "CPHLFLP1 [MICROGRAMS PER LITRE]",
        "CPHLPRKG [MICROGRAMS PER LITRE]",
        "CPHLZZXX [MILLIGRAMS PER CUBIC METRE]"
        ],
    "BotDepth": [
        "BOT. DEPTH [M]"
    ],
    "FloorDepth": [
        "SEA-FLOOR DEPTH [M]"
    ],
    "SampleDepth": [
        "DEPBELOWSURFACE [M]",
        "ADEPZZ01 [METRES]",
        "ADEPZZ01 [M]",
        "DEPTH OF SAMPLING [M]",
        "DEPTH [M]",
        "DEPTH BELOW SURFACE OF THE WATER BODY [M]",
        "ADEPZZ01 [METERS]"
    ]
}


_column_name_to_metadata = [
    "Cruise",
    "Station",
    "Type"
]


_column_name_quality = "QV:SEADATANET"


_datetime_regex_string = r"(?P<year>\d{4}).(?P<month>\d{1,2}).(?P<day>\d{1,2})[T|t]?(?P<hour>\d{1,2}).(?P<minute>\d{1,2}).(?P<second>\d{1,2})"
_datetime_regex = None



# OBJECT DEFINITIONS



class ValueObject(object):
    def __init__(self):
        self.value = None # the value of the cell in this position; can be numeric, a string value (text or datetime), or None if the value is missing
        self.quality = None # if the column had a corresponding QV:SEADATANET value, it is stored here
        self.valid = True # if False, the value had a QV:SEADATANET value that indicated an error (was not 1)
        self.copied = False # if True, the value was copied from the one above it during the processing of the data
        self.is_repeated: bool = False # if True, it has the same value as the one above

class ColumnObject(object):
    def __init__(self):
        self.name = None # the name of the column, can be one of several that are defined in the program
        self.original_name = None # the exact text that was used in the original file to name the column
        self.values = [] # the list of values
        self.repeating = False # if there was more than one measurement but the value of this column was only in the first line, the value was repeated and this value is True
        self.repeat_coefficient = None # the coefficient indicating how many sequential pairs of values are the same; 0 if none are the same, 1 if all are
        self.low_priority = False # if there are multiple columns of a type in the same dataObject, only one of them can be main, others are marked with low_priority = True
    def repeat_coefficient_recalculate(self) -> None: # recalculates the low_priority property
        previous = None
        count_all = 0
        count_repeat = 0
        for valueObject in self.values:
            if previous != None and valueObject.value == previous.value:
                count_repeat += 1
            count_all += 1
            previous = valueObject
        self.repeat_coefficient = None if count_all == 0 else (0 if count_all == 1 else count_repeat / (count_all - 1))
    def repeat_values_recalculate(self) -> None: # recalculates (and marks) the repeating values in its list of values
        if len(self.values) <= 1:
            return
        value_previous = self.values[0]
        for value in self.values[1:]:
            value.is_repeated = (value.value == value_previous.value)
            value_previous = value

class DataObject(object):
    def __init__(self):
        self.file_name = None
        self.column_list = []
        self.was_made_single = False
        self.valid = True
        self.metadata = {}
        self.settings = {}
    def is_single(self) -> bool | NoneType:
        if len(self.column_list) == 0:
            return None
        return len(self.column_list[0].values) == 1
    def check_if_valid(self) -> bool:
        column_name_set = set([columnObject.name for columnObject in self.column_list])
        if len(set(_column_name_required) - column_name_set) > 0:
            return False
        return len(self.column_list[0].values) > 0
    def __str__(self) -> str:
        s = []
        s.append("File name: '{:}'".format(self.file_name))
        s.append("Is valid: {:}".format(self.valid))
        s.append("Was made single: {:}".format(self.was_made_single))
        s.append("Metadata:")
        for key, value in self.metadata.items():
            s.append("    '{:}': '{:}'".format(key, value))
        s.append("Settings:")
        for key, value in self.settings.items():
            s.append("    '{:}': '{:}'".format(key, value))
        s.append("Table:")
        t = []
        tb = ["Original name:", "Is repeating:", "Low priority:", "Repeat coefficient:", "", "Column name:"]
        ml = max([len(x) for x in tb])
        tb = [("{:>" + str(ml) + "s}").format(val) for val in tb]
        tb[4] = "-"*ml
        tb.append("="*ml)
        for columnObject in self.column_list:
            c = []
            c.append(columnObject.original_name)
            c.append(str(columnObject.repeating))
            c.append(str(columnObject.low_priority))
            if type(columnObject.repeat_coefficient) == float or type(columnObject.repeat_coefficient) == int:
                c.append("{:.4f}".format(float(columnObject.repeat_coefficient)))
            else:
                c.append("")
            c.append("")
            c.append(columnObject.name)
            c.append("")
            for valueObject in columnObject.values:
                cell = []
                cell.append(" ")
                cell.append("q")
                cell.append(str(valueObject.quality) if valueObject.quality is not None else " ")
                cell.append(" ")
                cell.append("v")
                cell.append("T" if valueObject.valid else "F")
                cell.append(" ")
                cell.append("c")
                cell.append("T" if valueObject.copied else "F")
                cell.append(" ")
                cell.append("r")
                cell.append("T" if valueObject.is_repeated else "F")
                cell.append(" ")
                if type(valueObject.value) == str:
                    cell.append(valueObject.value)
                elif type(valueObject.value) == float or type(valueObject.value) == int:
                    cell.append("{:10.3f}".format(float(valueObject.value)))
                else:
                    cell.append("")
                cell.append(" ")
                cell_str = "".join([str(x) for x in cell])
                c.append(cell_str)
            ml = max([len(x) for x in c])
            c = [("{:^" + str(ml) + "s}").format(val) for val in c]
            c[4] = "-"*ml
            c[6] = "="*ml
            t.append(c)
        len_diff = min([len(x) for x in t]) - len(tb)
        tb = tb + [" "*len(tb[0]) for _ in range(len_diff)]
        t = [tb] + t
        table = [x for x in zip(*t)]
        s = s + ["|".join(line) for line in table]
        return "\n".join(s)




# FUNCTION DEFINITIONS


# stopwatch functions
def stopwatch_start() -> None:
    global _time_start
    _time_start = time.time()
def stopwatch_stop() -> float:
    global _time_start
    if (_time_start == None):
        raise Exception("Stopwatch needs to be started before it can be stopped!")
    time_now = time.time()
    time_diff = time_now - _time_start
    _time_start = time_now
    return time_diff
# formats the input time (in seconds) into a human-readable format
def time_format(seconds, precision: int = 3) -> str:
    string_format = "{1:02d}:{0:0" + str(precision + 3) + "." + str(precision) + "f}"
    h = int(seconds // 3600)
    m = int((seconds // 60) % 60)
    s = seconds % 60
    if h > 0:
        string_format = "{2:02d}:" + string_format
    return string_format.format(s, m, h)


# initialization
def main_init() -> None:
    global _time_start
    global _datetime_regex

    # set up the logger
    # https://docs.python.org/3/howto/logging.html
    logging.basicConfig(
        filename="log.txt",
        datefmt="%Y-%m-%d %H:%M:%S",
        format="[%(asctime)s] %(levelname)s: %(message)s",
        #encoding="UTF-8",
        level=logging.DEBUG
    )

    stopwatch_start()
    _logger.info("Script execution started.")

    _datetime_regex = re.compile(_datetime_regex_string)


# finalization
def main_finish() -> None:

    total_time = stopwatch_stop()
    _logger.info("Script execution finished in {0}.".format(time_format(total_time)))



# wrapper function for opening a file and serializing an object
def serialize(obj, filepath: str) -> None:
    f = open(filepath, "wb")
    pickle.dump(obj, f)
    f.close()

# wrapper function for opening a file and serializing an object
def deserialize(filepath: str) -> object:
    f = open(filepath, "rb")
    obj = pickle.load(f)
    f.close()
    return obj



# reads the files containing the filenames to all files that should be parsed
# returns the list of full filenames of those files
def get_filenames_to_be_parsed(filename_in: str) -> list:

    mode = "NONE"

    mark_dict = {
        "QUALITY": None,
        "SELECT": None,
        "FILEMARK": None
    }

    root = ""
    filenames_list = []

    f = open(filename_in, mode = "r", encoding = "UTF-8")
    lines = f.readlines()
    f.close()

    lines = [line.strip("\n").strip() for line in lines]

    for line in lines:
        if len(line) == 0 or line[0] == "#":
            # empty or commented line
            continue
        if line[0] == "[":
            # get mode or mark
            ri = line.find("]")
            if ri == -1:
                # no closing bracket
                continue
            mark_full = line[1:ri].upper()
            mark = mark_full.split(":")[0]
            mark_setting = None
            if ":" in mark_full:
                if len(mark_full) > len(mark) + 1:
                    mark_setting = mark_full.split(":")[1]
                    try:
                        mark_setting = float(mark_setting)
                    except:
                        mark_setting = mark_full.split(":")[1]
                else:
                    mark_setting = None
                mark_dict[mark] = mark_setting
            elif mark in ["NONE", "WHITELIST", "BLACKLIST", "ROOT"]:
                mode = mark
            continue
        # based on mode, do an action with the current line
        if mode == "NONE" or mode == "BLACKLIST":
            # no action yet specified, or the files are blacklisted
            continue
        elif mode == "ROOT":
            root = line
        elif mode == "WHITELIST":
            filepath = root + "/" + line
            prop = (filepath, {"QUALITY": mark_dict["QUALITY"], "SELECT": mark_dict["SELECT"], "FILEMARK": mark_dict["FILEMARK"]})
            filenames_list.append(prop)

    return filenames_list


# reads the .odv file containing the data
# returns the unprocessed values
def get_odv_file_contents(filepath_in: str) -> ty.Tuple[ty.List[str], ty.List[ty.List[str]]]:
    # read the file
    f = open(filepath_in, mode="r", encoding="UTF-8")
    lines = f.readlines()
    f.close()

    lines = [line.strip("\n") for line in lines]
    if lines[0].find("\ufeff") == 0:
        lines[0] = lines[0][1:]

    # remove all un-commented (that don't start with '//') and empty lines
    lines = [line for line in lines if line.find("//") != 0 or len(line) == 0]

    # split each line into individual entries
    entries = [line.split("\t") for line in lines]

    # parse the entries
    # the first line is the column definition
    columns = entries[0]
    # the following lines are values
    values = entries[1:]

    # transform value list from [line_num][column_index] to [column_index][line_num]
    values = [list(x) for x in zip(*values)]

    return (columns, values)
    

# maps column names into indexes, creating new indexes if necessary, returning an array of column indexes
def get_column_indexes(column_names: list) -> list:
    global _column_names
    column_indexes = []
    for column_name in column_names:
        if column_name not in _column_names:
            _column_names.append(column_name)
        column_index = _column_names.index(column_name)
        column_indexes.append(column_index)

# gets the name of the column with the given index
def get_column_name(column_index: int) -> str:
    global _column_names
    if column_index < 0 or column_index >= len(_column_names):
        _logger.warning("Trying to access column name by index, but given index is out of range. Column index: {0}".format(column_index))
        return None
    return _column_names[column_index]

# finds and returns the column index with the given name
def get_column_index(column_name: str) -> int:
    global _column_names
    if column_name not in _column_names:
        _logger.warning("Trying to access column index by name, but no column with the given name exists. Column name: '{0}'".format(column_name))
        return None
    return _column_names.index(column_name)


# parses the values inside the given column based on column name
def parse_column_values(column: ColumnObject) -> None:
    # get parse type
    parse_type = _column_parse_format_map[column.name]

    if parse_type == "str":
        for valueObject in column.values:
            if valueObject.value == "":
                valueObject.value = None
        return
    
    if parse_type == "float":
        for valueObject in column.values:
            if valueObject.value == None or valueObject.value == "":
                valueObject.value = None
                continue
            try:
                val = float(valueObject.value.replace(",", "."))
            except:
                val = None
                _logger.warning("Failed to parse value '{:}' into float for column '{:}'.".format(val, column.name))
            valueObject.value = val
        return
    
    if parse_type == "datetime":
        for valueObject in column.values:
            if valueObject.value is not None and valueObject.value != "":
                regex_match = _datetime_regex.match(valueObject.value)
                if regex_match is not None:
                    (year_str, month_str, day_str, hour_str, minute_str, second_str) = regex_match.groups()
                    year = int(year_str)
                    month = int(month_str)
                    day = int(day_str)
                    hour = int(hour_str)
                    minute = int(minute_str)
                    second = int(second_str)
                    if month > 12 or day > 31 or hour >= 24 and minute >= 60 or second >= 60:
                        _logger.warning("Parsed datetime is not correct: '{0:}'.".format(valueObject.value))
                    valueObject.value = "{0:04d}-{1:02d}-{2:02d}T{3:02d}:{4:02d}:{5:02d}".format(year, month, day, hour, minute, second)
        return

    return


# transforms the raw data (column names and values) into a table
def get_table(col_list: ty.List[str], val_list: ty.List[ty.List[str]]) -> DataObject:
    
    index = 0

    dataObject: DataObject = DataObject()
    columnObject: ColumnObject = ColumnObject()

    while(index < len(col_list)):
        col = col_list[index]
        col_upper = col.upper()
        vals = val_list[index]
        if col == _column_name_quality:
            if columnObject.name != None:
                # fill the values of the column with quality data
                for valueObject, quality in zip(columnObject.values, vals):
                    try:
                        quality_int = int(quality)
                    except:
                        quality_int = None
                    valueObject.quality = quality_int
        else:
            # add previous to list (if it exists):
            if columnObject.name != None:
                dataObject.column_list.append(columnObject)
            columnObject = ColumnObject()
            if col_upper in _column_name_map.keys():
                # create a new columnObject and fill with values
                columnObject = ColumnObject()
                columnObject.name = _column_name_map[col_upper]
                columnObject.original_name = col
                for val in vals:
                    valueObject = ValueObject()
                    valueObject.value = val
                    columnObject.values.append(valueObject)
        index += 1
    
    # check if last processed column has been added to the dataObject
    if columnObject != None and (columnObject.name != None and columnObject.name != dataObject.column_list[-1].name):
        dataObject.column_list.append(columnObject)
    
    # parse values into the correct format
    [parse_column_values(column) for column in dataObject.column_list]

    # return properly formatted values
    return dataObject
            
            


# reads the .odv file containing the data
# processes the values
# returns the processed values
def read_and_process_odv_file(filepath_in: str, options: dict) -> DataObject:
    global _column_names
    # get all of the data from the .odv file (unprocessed)
    (column_name_list, value_table) = get_odv_file_contents(filepath_in)

    # parse the data into a table
    data = get_table(column_name_list, value_table)

    # add the file name to the data object
    ri = filepath_in.replace("\\", "/").rfind("/")
    if ri != -1:
        data.file_name = filepath_in[(ri + 1):]
    
    data.settings = options

    return data
            

# checks if a given DataObject has everything necessary for it to be useful
def dataObject_is_useful(dataObject: DataObject) -> bool:
    return all([any([value != None and value != "" for value in column.values]) for column in dataObject.column_list if column.name in _column_name_required])


# find minimum index of value in an array
def min_index(l: list, f = lambda x : x) -> int:
    index = None
    min_val = None
    min_val_set = False
    for i, entry in enumerate(l):
        val = f(entry)
        if not min_val_set:
            index = i
            min_val = val
            min_val_set = True
        elif val < min_val:
            index = i
            min_val = val
    return -1 if not min_val_set else index


# get an array of indexes of the input array where the condition is satisfied
def filter_list_index(l: list, f = lambda _ : True) -> list:
    return [i for i, element in enumerate(l) if f(element)]


# get an array of modified elements
def select_list(l: list, f = lambda x : x) -> list:
    return [f(x) for x in l]


# returns True if value is None, NaN or empty string, True otherwise
def check_if_none(val):
    return val is None or val == "" or (type(val) == type(0.1) and not val == val)


# if input value is None (or NaN), replace with replacement value
def is_none(input_value, replacement_value):
    if check_if_none(input_value):
        return replacement_value
    return input_value


# remove duplicate columns from a dataObject using a priority list
def remove_duplicate_columns(dataObject: DataObject) -> None:
    for column_type, priority_list in _column_original_name_priority_list.items():
        matching_column_list = [x for x in dataObject.column_list if x.name == column_type]
        matching_column_score_list = select_list(matching_column_list, lambda columnObject : min_index(priority_list, lambda priority_value : priority_value != columnObject.original_name.upper()))
        primary_column_index = min_index(matching_column_score_list, lambda score : -score)
        for i, columnObject in enumerate(matching_column_list):
            if primary_column_index != -1 and i != primary_column_index:
                columnObject.low_priority = True
    dataObject.column_list = [columnObject for columnObject in dataObject.column_list if not columnObject.low_priority]



# tries to find the first column with the given name that has low_priority equal to that of the given value
# if none found, returns None, otherwise returns the first matching column
def try_get_column(
    dataObject: DataObject,
    column_name: str,
    low_priority_value: bool | NoneType = False
) -> ColumnObject:
    column_list = [col for col in dataObject.column_list if col.name == column_name and (low_priority_value is None or col.low_priority == low_priority_value)]

    if len(column_list) == 0:
        return None
    
    return column_list[0]

# tries to find the first column with the given name that has low_priority equal to that of the given value
# if none found, returns the first replacement value
# if found, tries to get the value at the given index
# if value is None (or satisfies the given optional condition), returns the second replacement value
# if value is not None (or doesn't satisfy the optional condition), returns the value (through the given function)
def try_get_column_value(
    dataObject: DataObject,
    column_name: str,
    index: int,
    replacement_value_no_columns_found: str = "",
    replacement_value_value_none: str = "",
    condition: FunctionType = lambda x : x is None,
    out_function: FunctionType = lambda x : str(x),
    low_priority_value: bool | NoneType = False
):
    column = try_get_column(dataObject, column_name, low_priority_value)
    if column is None:
        return replacement_value_no_columns_found

    value = column.values[index].value
    if condition(value):
        return replacement_value_value_none
    
    return out_function(value)



# calculates the median value of a list (with optional lambda function) and returns it
# for empty list, None is returned
# for a list with an even number of elements the average of the middle two values is returned
def median(l: list, f = lambda x : x):
    if type(l) != type([]):
        return None
    ln = len(l)
    if ln == 0:
        return None
    median_values_sorted_list = sorted([f(x) for x in l])
    if ln % 2 == 1:
        return median_values_sorted_list[ln // 2]
    return (median_values_sorted_list[ln // 2 - 1] + median_values_sorted_list[ln // 2]) / 2


# gets the total number of valid measurements from a dataObject
def get_valid_count(dataObject: DataObject) -> int:
    if len(dataObject.column_list) == 0:
        return 0
    total = 0
    l = len(dataObject.column_list[0].values)
    for i in range(l):
        for column in [col for col in dataObject.column_list if col.name in _column_name_required]:
            if not column.values[i].valid:
                break
        else:
            total += 1
    return total



# process and improve data
def process_and_improve_data(dataObject: DataObject, seq: int | NoneType = None) -> DataObject:

    _logger.info("Started processing file #{:}.".format(seq))
    for columnObject in dataObject.column_list:
        columnObject.repeat_values_recalculate()
        columnObject.repeat_coefficient_recalculate()
    if setting_logger_print_table_after_each_step or setting_logger_print_table_at_start:
        _logger.debug("DataObject:\n{:s}".format(str(dataObject)))

    _logger.info("File has {:} valid measurements.".format(get_valid_count(dataObject)))
    if not dataObject.check_if_valid():
        dataObject.valid = False
        _logger.warning("This file has been detected as invalid and will not be included in the result.")
        return dataObject
    

    # remove 'duplicate' columns using a priority list
    remove_duplicate_columns(dataObject)
    _logger.info("{:} valid measurements: Done removing duplicate columns.".format(get_valid_count(dataObject)))
    if setting_logger_print_table_after_each_step:
        _logger.debug("DataObject:\n{:s}".format(str(dataObject)))
    if not dataObject.check_if_valid():
        dataObject.valid = False
        _logger.warning("This file has been detected as invalid and will not be included in the result.")
        return dataObject
    

    # remove columns that are not required, put the information from other columns in the metadata of the dataObject
    for column_name in _column_name_to_metadata:
        columnObject = try_get_column(dataObject, column_name)
        if columnObject is not None and len(columnObject.values) > 0:
            dataObject.metadata[column_name] = columnObject.values[0].value
    dataObject.column_list = [columnObject for columnObject in dataObject.column_list if columnObject.name not in _column_name_to_metadata]
    _logger.info("{:} valid measurements: Done removing columns that are not required.".format(get_valid_count(dataObject)))
    if setting_logger_print_table_after_each_step:
        _logger.debug("DataObject:\n{:s}".format(str(dataObject)))
    if not dataObject.check_if_valid():
        dataObject.valid = False
        _logger.warning("This file has been detected as invalid and will not be included in the result.")
        return dataObject


    # mark Chl values that are 0 or negative as invalid
    column_chl = try_get_column(dataObject, "Chl")
    if column_chl is not None:
        for val in column_chl.values:
            if val.value is None or val.value <= 0:
                val.valid = False
    _logger.info("{:} valid measurements: Done marking values with negative or zero Chl values as invalid.".format(get_valid_count(dataObject)))
    if setting_logger_print_table_after_each_step:
        _logger.debug("DataObject:\n{:s}".format(str(dataObject)))
    if not dataObject.check_if_valid():
        dataObject.valid = False
        _logger.warning("This file has been detected as invalid and will not be included in the result.")
        return dataObject


    # if QV:SEADATANET is specified, allow only data where the quality value is 1 or 2
    for columnObject in dataObject.column_list:
        if columnObject.name != "Chl":
            # only Chl columns are filtered by their quality
            continue
        for valueObject in columnObject.values:
            if valueObject.quality != None and valueObject.quality not in (1, 2):
                valueObject.valid = False
    _logger.info("{:} valid measurements: Done excluding values that have their quality specified and the quality is not acceptable.".format(get_valid_count(dataObject)))
    if setting_logger_print_table_after_each_step:
        _logger.debug("DataObject:\n{:s}".format(str(dataObject)))
    if not dataObject.check_if_valid():
        dataObject.valid = False
        _logger.warning("This file has been detected as invalid and will not be included in the result.")
        return dataObject
    

    # remove all lines with invalid Chl data
    column_chl = try_get_column(dataObject, "Chl")
    if column_chl is not None:
        keep_index_list = filter_list_index(column_chl.values, lambda value : value.valid)
        for column in dataObject.column_list:
            column.values = [column.values[index] for index in keep_index_list]
            column.repeat_coefficient_recalculate()
    _logger.info("{:} valid measurements: Done removing all measurements with invalid Chl data.".format(get_valid_count(dataObject)))
    if setting_logger_print_table_after_each_step:
        _logger.debug("DataObject:\n{:s}".format(str(dataObject)))
    if not dataObject.check_if_valid():
        dataObject.valid = False
        _logger.warning("This file has been detected as invalid and will not be included in the result.")
        return dataObject
    
    
    # check for outliers in measurements
    # remove all Chl values with a z-score of more than a specified threshold
    columnObject_chl = [columnObject for columnObject in dataObject.column_list if columnObject.name == "Chl"]
    if len(columnObject_chl) > 0:
        columnObject_chl = columnObject_chl[0]
        float_nan_value = float("NaN")
        if len(columnObject_chl.values) == 0:
            z_score_accept = []
        elif len(columnObject_chl.values) == 1:
            z_score_accept = [0]
        else:
            z_score_list = stats.zscore(select_list(columnObject_chl.values, lambda valueObject : is_none(valueObject.value, float_nan_value)), nan_policy="omit")
            z_score_accept = filter_list_index(z_score_list, lambda x : True if x == float_nan_value else abs(x) < setting_z_score_threshold)
        # filter all measurements, keep only those whose index is in z_score_accept
        for columnObject in dataObject.column_list:
            columnObject.values = [columnObject.values[i] for i in z_score_accept]
            columnObject.repeat_coefficient_recalculate()
    _logger.info("{:} valid measurements: Done removing outlier values.".format(get_valid_count(dataObject)))
    if setting_logger_print_table_after_each_step:
        _logger.debug("DataObject:\n{:s}".format(str(dataObject)))
    if not dataObject.check_if_valid():
        dataObject.valid = False
        _logger.warning("This file has been detected as invalid and will not be included in the result.")
        return dataObject


    # repeat measurements if they are missing - copy them
    for columnObject in dataObject.column_list:
        latest_value = None
        for valueObject in columnObject.values:
            if not check_if_none(valueObject.value):
                latest_value = valueObject
            elif not check_if_none(latest_value):
                valueObject.value = latest_value.value
                valueObject.quality = latest_value.quality
                valueObject.valid = latest_value.valid
                valueObject.copied = True
                valueObject.is_repeated = True
            else:
                valueObject.valid = False
        # if the entire column has repeating values, save this information into the columnObject
        if len(columnObject.values) > 1 and len(columnObject.values) - 1 == len([value for value in columnObject.values if value.copied]):
            columnObject.repeating = True
    _logger.info("{:} valid measurements: Done repeating missing measurements.".format(get_valid_count(dataObject)))
    if setting_logger_print_table_after_each_step:
        _logger.debug("DataObject:\n{:s}".format(str(dataObject)))
    if not dataObject.check_if_valid():
        dataObject.valid = False
        _logger.warning("This file has been detected as invalid and will not be included in the result.")
        return dataObject


    # if time (dateTime) and location (lon, lat) are constant between consecutive measurements,
    # select only one measurement based off of a priority list
    # first, calculate the repeat coefficient
    for columnObject in dataObject.column_list:
        columnObject.repeat_coefficient_recalculate()
    # then check what the changing column is, and select only one measurement
    dataObject_is_single = dataObject.is_single()
    if check_if_none(dataObject_is_single):
        dataObject.valid = False
    elif not dataObject_is_single:
        # get columns
        columnObject_dateTime = try_get_column(dataObject, "DateTime")
        columnObject_lon = try_get_column(dataObject, "Lon")
        columnObject_lat = try_get_column(dataObject, "Lat")
        # check if the values are repeating
        if (not check_if_none(columnObject_dateTime)) and columnObject_dateTime.repeat_coefficient == 1 and \
            (not check_if_none(columnObject_lon)) and columnObject_lon.repeat_coefficient == 1 and \
            (not check_if_none(columnObject_lat)) and columnObject_lat.repeat_coefficient == 1:
            # values are repeating, select the measurement with the most preferable circumstances
            _logger.info("'{:}' contains locationally and temporally invariant measurements. Selecting only one measurement.".format(dataObject.file_name))
            # get columns
            columnObject_sampleDepth = try_get_column(dataObject, "SampleDepth")
            columnObject_floorDepth = try_get_column(dataObject, "FloorDepth")
            columnObject_botDepth = try_get_column(dataObject, "BotDepth")
            if not check_if_none(columnObject_sampleDepth) and any([not check_if_none(valueObject.value) for valueObject in columnObject_sampleDepth.values]):
                # selecting based on lowest SampleDepth value
                min_sampleDepth_index = min_index(columnObject_sampleDepth.values, lambda x : x.value)
                # keep the measurement with the lowest sample depth and remove all others
                for columnObject in dataObject.column_list:
                    keep_value = columnObject.values[min_sampleDepth_index]
                    columnObject.values = [keep_value]
                dataObject.was_made_single = True
            elif not check_if_none(columnObject_floorDepth) and any([not check_if_none(valueObject.value) for valueObject in columnObject_floorDepth.values]):
                # selecting based on lowest FloorDepth value
                min_floorDepth_index = min_index(columnObject_floorDepth.values, lambda x : x.value)
                # keep the measurement with the lowest floor depth and remove all others
                for columnObject in dataObject.column_list:
                    keep_value = columnObject.values[min_floorDepth_index]
                    columnObject.values = [keep_value]
                dataObject.was_made_single = True
            elif not check_if_none(columnObject_botDepth) and any([not check_if_none(valueObject.value) for valueObject in columnObject_botDepth.values]):
                # selecting based on lowest BotDepth value
                min_botDepth_index = min_index(columnObject_botDepth.values, lambda x : x.value)
                # keep the measurement with the lowest bot depth and remove all others
                for columnObject in dataObject.column_list:
                    keep_value = columnObject.values[min_botDepth_index]
                    columnObject.values = [keep_value]
                dataObject.was_made_single = True
            else:
                # there were no columns with changing values found, select the median chl value and the first line from other columns
                columnObject_chl = try_get_column(dataObject, "Chl")
                if columnObject_chl is not None:
                    chl_median = median([x.value for x in columnObject_chl.values if not check_if_none(x.value)])
                    columnObject_chl.values[0].value = chl_median
                    # for all columns keep only the first measurement
                    for columnObject in dataObject.column_list:
                        columnObject.values = [columnObject.values[0]]
                    dataObject.was_made_single = True
                else:
                    # invalidate the entire dataObject
                    dataObject.valid = False
    _logger.info("{:} valid measurements: Done filtering out repeating values.".format(get_valid_count(dataObject)))
    if setting_logger_print_table_after_each_step:
        _logger.debug("DataObject:\n{:s}".format(str(dataObject)))
    if not dataObject.check_if_valid():
        dataObject.valid = False
        _logger.warning("This file has been detected as invalid and will not be included in the result.")
        return dataObject

    
    # if a dataObject has Chl columnObject with repeating_coef of more than a threshold (and more than n measurements),
    # remove all duplicate measurements from the dataObject
    column_chl = try_get_column(dataObject, "Chl")
    if column_chl is not None:
        column_chl.repeat_values_recalculate()
        if len(column_chl.values) > 0 and column_chl.repeat_coefficient >= setting_filter_repeat_coefficient_threshold:
            keep_index_list = filter_list_index(column_chl.values, lambda value : not value.is_repeated)
            for column in dataObject.column_list:
                column.values = [column.values[index] for index in keep_index_list]
                column.repeat_values_recalculate()
        _logger.info("{:} valid measurements: Done removing measurements with sequential repeating chl values that have a repeat coefficient of at least {:}."
            .format(get_valid_count(dataObject), setting_filter_repeat_coefficient_threshold))
    if setting_logger_print_table_after_each_step or setting_logger_print_table_at_end:
        _logger.debug("DataObject:\n{:s}".format(str(dataObject)))
    if not dataObject.check_if_valid():
        dataObject.valid = False
        _logger.warning("This file has been detected as invalid and will not be included in the result.")
        return dataObject



    # return processed data
    return dataObject





# save the parsed data
def save_data(dataObjects: list, filename: str) -> None:

    nl = "\n"
    c = ","

    f = open(filename, "w", encoding="UTF-8")

    # write the header
    f.write(",".join(_column_names_out_list) + nl)

    counter = 0

    # iterate through each dataObject and write its contents to the file
    for id, dataObject in enumerate(dataObjects, start=1):
        total = len(dataObject.column_list[0].values)
        for i in range(total):
            counter += 1
            text_list = []
            text_list.append(str(id)) # file_id - same for each measurement inside an individual file
            text_list.append(str(i + 1)) # seq_in - different for each measurement in a file
            text_list.append(str(total)) # total_in - number of measurements inside each file
            text_list.append(str(counter)) # seq_all - counts each line in the output file
            text_list.append(try_get_column_value(dataObject, "DateTime", i)) # date_time - date and time of the measurement
            column_datetime = try_get_column(dataObject, "DateTime") # exact_datetime - is the datetime exact
            if column_datetime is None:
                text_list.append("")
            else:
                text_list.append("N" if column_datetime.repeating else "Y")
            text_list.append(try_get_column_value(dataObject, "Lon", i)) # lon - longitude
            text_list.append(try_get_column_value(dataObject, "Lat", i)) # lat - latitude
            text_list.append(try_get_column_value(dataObject, "Chl", i)) # chl - chlorophyll-a concentration
            column_chl = try_get_column(dataObject, "Chl") # chl_repeat_coef - chlorophyll-a repeat coefficient
            if column_chl is None:
                text_list.append("")
            else:
                text_list.append(str(column_chl.repeat_coefficient))
            text_list.append(try_get_column_value(dataObject, "BotDepth", i)) # bot_depth
            text_list.append(try_get_column_value(dataObject, "FloorDepth", i)) # floor_depth - the depth of the ocean floor
            text_list.append(try_get_column_value(dataObject, "SampleDepth", i)) # sample_depth - the depth below the water surface at which the sample was taken
            text_list.append(str(dataObject.settings["QUALITY"]) if dataObject.settings["QUALITY"] is not None else "")
            text_list.append(str(dataObject.settings["FILEMARK"]) if dataObject.settings["FILEMARK"] is not None else "")

            f.write(",".join(text_list) + nl)

    f.close()




# main function
def main() -> None:

    # initialization
    main_init()
    
    # check if there are any input arguments
    sys.argv.append(_input_filename)
    sys.argv.append(_input_filename_out)

    if len(sys.argv) < 3:
        _logger.critical("No input arguments entered!")
        exit(1)

    # read the input arguments
    filename_with_input_files = sys.argv[1]
    filename_out = sys.argv[2]

    # get the list of all files to be parsed
    file_object_to_be_parsed_list = get_filenames_to_be_parsed(filename_with_input_files)
    _logger.info("Found {:} files containing data.".format(len(file_object_to_be_parsed_list)))

    # check if a serialized version od parsed file data already exists
    # if it exists, use that, otherwise generate (and save) a new one
    data_list = []
    if setting_serialize_and_deserialize_parsed_data and os.path.exists(_data_raw_serialized_filename):
        data_list = deserialize(_data_raw_serialized_filename)
        amount1 = len(data_list)
    else:
        # get the processed contents of the odv file
        for i, (file_full_path, file_dict) in enumerate(file_object_to_be_parsed_list, start = 1):
            _logger.info("Reading and processing data from {:}/{:} file: '{:}'".format(i, len(file_object_to_be_parsed_list), file_full_path))
            filedata = read_and_process_odv_file(file_full_path, file_dict)
            _logger.info("File parsed.")
            data_list.append(filedata)
        amount1 = len(data_list)
        _logger.info("Completed first stage of data processing for {:} files.".format(amount1))
        # remove files with low quality
        data_list = [d for d in data_list if "QUALITY" in d.settings and type(d.settings["QUALITY"]) in (int, float) and d.settings["QUALITY"] >= setting_quality_min_threshold]
        amount2 = len(data_list)
        _logger.info("{:} files were excluded because their quality was below specified, {:} files remain.".format(amount1 - amount2, amount2))
        if setting_serialize_and_deserialize_parsed_data:
            serialize(data_list, _data_raw_serialized_filename)

    # filter out useless data (has to have at lease one valid measurement of datetime, lon, lat, and chl each)
    data_list = [d for d in data_list if dataObject_is_useful(d)]
    amount3 = len(data_list)
    _logger.info("Filtered out {:}/{:} processed files because they did not contain any useful data. {:} files remain.".format(amount1 - amount3, amount1, amount3))

    # if the option for selecting only the first line is active, select only the first line
    amount4 = 0
    for dataObject in data_list:
        if dataObject.settings["SELECT"] == "FIRST" and len(dataObject.column_list[0].values) > 0:
            amount4 += 1
            for columnObject in dataObject.column_list:
                columnObject.values = [columnObject.values[0]]
            dataObject.was_made_single = True
    if amount4 > 0:
        _logger.info("Made {:} files only use their first measurement.".format(amount4))

    # process the data into a more useful form
    data_list = [process_and_improve_data(d, i) for i, d in enumerate(data_list, start = 1)]
    data_list = [x for x in data_list if x.valid]

    # save the parsed data to a file
    save_data(data_list, filename_out)

    # finalization
    main_finish()






if __name__ == "__main__":
    main()