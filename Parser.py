from DataAggregator import *
import re
import datetime
from itertools import cycle
import pickle


# TODO add support for non-numeric values
# TODO enable "NA" encoding and/ or interpolation for missing time frames


class LineParser(object):
    """
        Provides methods to parse a line of a csv like file, extracting the timestamp and inserting all extracted
        numerical values in a tree structure. At moment values are always stored in minute resolution
    """
    def __init__(self, container_object, columns, timestamp_column=-1, sensor="", sep=",", hour_format="auto",
                 date_format="auto"):
        # type: (dict, int, int, str, str, str, str) -> None
        """
        Constructor
        :param container_object: (a reference to a dictionary given by a FileParser instance)
        :param timestamp_column: the column of the line after splitting, where the timestamp can be found (-1 = auto)
        :param sensor: name of the used sensor e.g. "ECG" (optional)
        :param sep: separator for csv like file that defines the individual fields (default: ",")
        :param columns: list of column indices defining which fields to parse (default: all)
        :param hour_format: predefines the hour-format (12/24) used throughout the file (default: "auto")
        :param date_format: predefines the date-format (D/M/Y or Y/M/D) used throughout the file (default: "auto") if
                            american date format is used user must indicate it by giving the parameter ("US")
        """
        self._sep = sep
        self._columns = columns
        self._time_format = hour_format # int 12 or int 24
        self._date_format = date_format # str year or str day - day=day_month_year; year=year_mont_day
        self._is_initialized = False
        self._date_pattern = None
        self._time_pattern = None
        self._col_names = []
        self.container = container_object
        self._sensor = sensor
        self._time_stamp_column = timestamp_column

    def __str__(self):
        # type: (None) -> str
        """
        :return: Object Status/ Settings
        """
        return "LineParser \nSettings: Separator='%s'; Hour Format=%s; Date Format=%s" % (self._sep, self._time_format,
                                                                                          self._date_format)

    def set_date_pattern(self):
        # type: (None) -> None
        """
        Sets the date regexp pattern
        :return:
        """

        if self._date_format == "year":
            self._date_pattern = r'(\d{4})\S(\d{2})\S(\d{2})'
        else:
            self._date_pattern = r'(\d{2})\S(\d{2})\S(\d{4})'

    def set_time_pattern(self):
        # type: (None) -> None
        """
        Sets the time regexp pattern
        :return:
        """

        if self._time_format == 12:
            self._time_pattern = r'\s(\d{2}):(\d{2}).*(am|AM|pm|PM)'
        else:
            self._time_pattern = r'\s(\d{2}):(\d{2})'

    def auto_detect_time_format(self, line):
        # type: (str) -> None
        """
        Automatically detects the time format of the file by the given line
        :param line: First line after header
        :return:
        """

        match = re.search(r'\sam|\sAM|\spm|\sPM\s', line)
        if match:
            self._time_format = 12
        else:
            self._time_format = 24

        self.set_time_pattern()

    def auto_detect_date_format(self, line):
        # type: (str) -> None
        """
        Automatically detects the date format of the file by the given line
        :param line: first line after header
        :return:
        """

        match = re.search(r'\d{4}\S\d{2}\S\d{2}', line)
        if match:
            self._date_format = "year"
        else:
            self._date_format = "day"
        self.set_date_pattern()

    def auto_detect_timestamp_column(self, line):
        # type: (str) -> None
        """
        :param line:
        :return:
        """
        chunks = line.split(self._sep) # bad practice, should add chunks as an instance variable!
        timestamp_col_index = None
        for chunk_idx in range(len(chunks)):
            if re.search(self._time_pattern, chunks[chunk_idx]) and re.search(self._date_pattern, chunks[chunk_idx]):
                timestamp_col_index = chunk_idx
                break
        if timestamp_col_index is not None:
            self._time_stamp_column = timestamp_col_index
        else:
            raise Exception("No timestamp found! Try setting it manually")

    def initialize(self, line):
        # type: (str) -> None
        """
        Is called at at the beginning of a file to initialize instance variables not yet set
        :param line:
        :return:
        """
        if self._time_format == "auto":
            self.auto_detect_time_format(line)
        else:
            self.set_time_pattern()

        if self._date_format == "auto":
            self.auto_detect_date_format(line)
        else:
            self.set_date_pattern()

        if self._time_stamp_column == -1:
            self.auto_detect_timestamp_column(line)

        for name in self._col_names:
            self.container[name] = MainContainer(sensor=self._sensor, _type=name)
        self._is_initialized = True

    def parse_header(self, line):
        # type: (str) -> None
        """
        :param line:
        :return:
        """
        col_names = line.split(self._sep)
        for i in range(len(col_names)):
            col_names[i] = col_names[i].strip()
        self._col_names = col_names
        self.set_col_index()

    def set_col_index(self):
        # TODO: add support for lists of names
        # type: (None) -> [int]
        """
        gets column index based on col name if it is defined and not a list
        :return:
        """
        if self._columns is not None and not isinstance(self._columns, list):
            for i in range(len(self._col_names)):
                if self._col_names[i] == self._columns:
                    self._columns = i
                    break

    def parse_line(self, line):
        # TODO: support for list of col names
        # type: (str) -> None
        """
        :param line:
        :return:
        """
        chunks = line.split(self._sep)

        if not self._is_initialized:
            self.initialize(line)
        if self._columns is None:
            n_cols = len(chunks)
            for col in range(n_cols):
                if col != self._time_stamp_column:
                    self.parse_value(chunks, col)
        #else:
        #    for col in range(self._columns):
        #        self.parse_value(chunks, self._time_stamp_column, col)
        else:
            self.parse_value(chunks, self._columns)

    def parse_value(self, line, column):
        # type: ([str], int) -> None
        """
        Parses one field of the csv file and adds it at the correct position in the container object
        :param line: actual csv line
        :param self._time_stamp_column:
        :param column: column index of the value to be extracted
        :return:
        """
        timestamp = self.extract_timestamp(line[self._time_stamp_column])
        try:
            value = self.extract_values(line[column])
        except:
            print("Could not be converted to numerical value (float): '" + line[column] +
                  "' Value has been coded as 'NA'")
            value = "NA"

        #insert value based on its date and column_name
        self.insert_value(self.container[self._col_names[column]], timestamp, value)

    def extract_values(self, input_str):
        # type: (str) -> [float]
        """
        Matches both integers and floating point numbers in the given string and return list of respecting floats
        :param input_str:
        :return:
        """
        values = []
        match_iter = re.findall(r'([0-9]*[\.,]?[0-9]+)', input_str)
        for match in match_iter:
            values.append(float(match))
        return values

    def insert_value(self, _object, timestamp, value):
        # type: (Container, datetime, [float]) -> None
        """
        Recursive method, traversing the tree, adding nodes/ leaves when necessary and inserting values at the
        correct position (each timestamp will be associated with one specific minute)
        :param _object: Instance of class Day, Hour or Minute
        :param timestamp:
        :param value:
        :return:
        """
        if len(value) < 1:  # don't add if no value is present
            return
        if _object.name == "minute":  # Base case
            _object.add_child(value)
            return
        if len(_object.children) < 1:  # If not leaf node and no children, create child node and traverse it
            _object.add_child(timestamp )
            self.insert_value(_object.children[-1], timestamp, value)
        else: # len(children) >= 1
            if _object.children[-1] == timestamp:  # enter last child node when no new time unit began
                self.insert_value(_object.children[-1], timestamp, value)
            else: # new time unit began, thus create new node/ leaf
                _object.add_child(timestamp)
                self.insert_value(_object.children[-1], timestamp, value)

    """extracts timestamp, parameter = str (line containing date)
       return = DateTime (timestamp)"""
    def extract_timestamp(self, line):
        # type: (str) -> datetime
        """
        Converts the csv's timestamp field to a datetime object which is defined up to the minute resolution
        :param line: timestamp string
        :return: datetime object
        """
        match = re.search(self._date_pattern, line)  # Extract date
        if self._date_format == "day":
            day = int(match.group(1))
            month = int(match.group(2))
            year = int(match.group(3))
        elif self._date_format == "year":
            day = int(match.group(3))
            month = int(match.group(2))
            year = int(match.group(1))
        else:
            day = int(match.group(2))
            month = int(match.group(1))
            year = int(match.group(3))

        #Extract time
        match = re.search(self._time_pattern, line)
        hour = int(match.group(1))
        minute = int(match.group(2))

        if self._time_format == 12:
            if re.search(r'pm|PM', line):
                #If 12h format convert to 24h format
                hour += 12
            if hour == 12  and re.search(r'am|AM', line):
                hour = 0  # Now I see why the 24h format is so much better...

        timestamp = datetime.datetime(year, month, day, hour, minute)
        return timestamp


# TODO: Add option for header position or no header
class FileReader(object):
    """
    Class able to handle a csv file (parse it and convert (selected) numerical content in a time based tree structure
    """
    @staticmethod
    def warning():
        print("If you use US date format it's impossible to auto detect so manually indicate it as "
              "date_format='US'")

    # TODO add col index of timestamp to constructor arguments
    def __init__(self, file_path, timestamp_column=-1, sep=",", sensor="", columns=None, hour_format="auto", date_format="auto"):
        # type: (str, str, str, [int], str, str) -> None
        """
        Constructor
        :param file_path: absolute or relative file path
        :param timestamp_column: column index of the timestamp after splitting the csv by the separator (default: -1 = auto)
        :param sensor: name of the used sensor e.g. "ECG" (optional)
        :param sep: separator for csv like file that defines the individual fields (default: ",")
        :param columns: list of column indices defining which fields to parse (default: all)
        :param hour_format: predefines the hour-format (12/24) used throughout the file (default: "auto")
        :param date_format: predefines the date-format (D/M/Y or Y/M/D) used throughout the file (default: "auto")
        """
        FileReader.warning()
        self.file_path = file_path
        self.container = {}
        self.line_parser = LineParser(self.container, columns, sensor=sensor, sep=sep, hour_format=hour_format,
                                      date_format=date_format, timestamp_column=timestamp_column)

    def __iadd__(self, other):
        self.file_path = other
        return self

    def read_file(self):
        # type: (None) -> None
        """
        Opens file and parses it
        :return:
        """
        with open(self.file_path) as file:
            self.line_parser.parse_header(next(file))
            for line in file:
                self.line_parser.parse_line(line)

    def load(self, file_address):
        # type: (str) -> None
        """
        Load previously saved container object
        :param file_address:
        :return:
        """
        self.container = pickle.load(file_address)

    def save(self, filename):
        # type: (str) -> None
        """
        Save container object (containing the extracted data) to disk
        :return:
        """
        pickle.dump(self.container, filename)



class FileWriter(object):
    """
    Provides methods to write a csv or json file off of the datetime tree structure created by a FileReader instance
    """
    @staticmethod
    def find_item_in_list(sorted_list, idx_left, idx_right, target):
        # type: ([], int, int, datetime) -> int
        """
        Performs simple binary search on the given ordered list
        Note: Might be changed to a iterative version
        :param sorted_list:
        :param idx_left: usually starts with 0
        :param idx_right: usually starts with len(sorted_list)
        :param target: target value to be found in the list
        :return: index of the target element in the array or -1 if not found
        """
        middle = (idx_left + idx_right) // 2
        if sorted_list[middle] == target:
            return middle
        if len(sorted_list[idx_left:idx_right]) <= 1:
            raise ValueError("File is probably not ordered chronologically!")
        if sorted_list[middle] > target:
            return FileWriter.find_item_in_list(sorted_list, idx_left, middle, target)
        else:
            return FileWriter.find_item_in_list(sorted_list, middle, idx_right, target)

    @staticmethod
    def dump(string, address="Undefined.csv"):
        # type: (str, str) -> None
        """
        Writes string into specified file
        :param string:
        :param address:
        :return:
        """
        with open(address, "w") as file:
            file.write(string)

    @staticmethod
    def get_string(value):
        # type: ([*float]) -> str
        """
        Checks whether value is a single value or a list and unpacks list into a single string in case
        :param value:
        :return: a string of the input value
        """
        if not isinstance(value, list):
            return str(value)
        else:
            string = ""
            for element in value:
                string += str(element) + " "
            return string

    @staticmethod
    def format_csv(values, col_name, sep):
        # type: ([*tuple], str, str) -> str
        """
        Processes list of tuples into a csv format string
        :param values: list of tuples (datetime, value)
        :param col_name: name of extracted column
        :param sep: the separator to be used
        :return: string form of the file
        """
        result = ""
        result += "Date%s %s\n" % (sep, col_name)
        for value in values:
            temp_line = "%s%s %s\n" % (str(value[0]), sep, FileWriter.get_string(value[1]))
            result += temp_line
        return result

    @staticmethod
    def format_json(values, col_name, sep):
        result = "[\n"
        for value in values:
            date = value[0]
            data = value[1]
            result += '{"Date": "%s", "Data": %s},\n' % (str(date), str(data))
        result = result[0:-2]
        result += "\n]"
        return result

    @staticmethod
    def string_to_datetime(str_date_time):
        date_list = str_date_time.split(":")
        l = []
        for i in range(5):
            try:
                l.append(int(date_list[i]))
            except IndexError:
                l.append(0)
                print("Unspecified values were set to 0")
        datetime_object = datetime.datetime(l[0], l[1], l[2], l[3], l[4])
        return datetime_object

    # TODO: Implement continuous time line
    """
    @staticmethod
    def range_time(start_time, end_time, resolution):
        # type: (datetime, datetime, str) -> iter
        '''
        returns the datetime range as an iterable
        :param start_time:
        :param end_time:
        :param resolution:
        :return:
        '''
        pass

    @staticmethod
    def process_values(values, function):
        string = ""
        for value in values:
            string += function(value)

    @staticmethod
    def minute_generator():
        mins = range(59)
        for i in cycle(mins):
            yield i

    @staticmethod
    def hour_generator(self):
        hours = range(24)
        for i in cycle(hours):
            yield i
    """
    def __init__(self, container):
        self.container = container

    def write(self, output_file, start_time, end_time, col_name, sep=",", resolution="minute", aggregate_type="mean",
              output_format="csv"):
        # type: (str, str, str, str, str, str, str, str) -> None
        """
        Convertes the container object into a csv like file and writes it to disk.
        :param output_file: filename or path including filename where output should be written to
        :param start_time: yyyy:mm:hh:mm
        :param end_time: yyyy:mm:hh:mm
        :param col_name: name of the column to be extracted
        :param sep: separator to be used, default ','
        :param resolution: desired output resolution atm supports "day", "hour" and "minute", default="minute"
        :param aggregate_type: How data should be aggregated (if possible) atm supports "mean", "min", "max"
        :param output_format: specifies the desire format of the output file ("csv" or "json")
        :param raw_values: Indicates whether values should be aggregated
        :return:
        """
        start_time = FileWriter.string_to_datetime(start_time)
        end_time = FileWriter.string_to_datetime(end_time)
        if aggregate_type != "none":
            values = self.get_aggregated_values(self.container[col_name], start_time=start_time, end_time=end_time,
                                                resolution=resolution, value_type=aggregate_type)
        else:
            values = self.get_raw_values(self.container[col_name], start_time=start_time, end_time=end_time)

        if output_format == "csv":
            result = FileWriter.format_csv(values, col_name, sep).rstrip()
        else:
            result = FileWriter.format_json(values, col_name, sep).rstrip()
        FileWriter.dump(result, output_file)
        return result

    def get_aggregated_values(self, objct, start_time, end_time, resolution, value_type="mean"):
        # type: (MainContainer, datetime, datetime, str, str) -> [*(datetime, *float)]
        """
        Recursively calculates aggregated values from a certain date to a certain date with a certain resolution
        :param objct: reference to a MainContainer instance "representing kind of the root node"
        :param start_time: datetime object defining the start time, resolution must at least match the desire resolution
        :param end_time: datetime object defining the end time, resolution must at least match the desi re resolution
        :param resolution: at which resolution data should be aggregated (day, hour, minute)
        :param value_type: type of aggregation ("mean", "min", "max")
        :return: list of tuples -> [*(datetime: timestamp, float: value)]
        """
        current_list = objct.children
        # get start and end index, if start/ end time is within current_list, else use whole list
        if current_list[0] > start_time:
            start_idx = 0
        else:
            start_idx = self.find_item_in_list(current_list, 0, len(current_list), start_time)
        if current_list[-1] < end_time:
            end_idx = len(current_list) - 1
        else:
            end_idx = self.find_item_in_list(current_list, 0, len(current_list), end_time)

        # Base case
        if current_list[start_idx].name == resolution:
            values = []
            for i in range(start_idx, end_idx + 1):
                if value_type == "mean":
                    value = (current_list[i].date, current_list[i].get_value())
                elif value_type == "min":
                    value = (current_list[i].date, current_list[i].get_min_value())
                elif value_type == "max":
                    value = (current_list[i].date, current_list[i].get_max_value())
                else:
                    raise AssertionError("Invalid value type: " + str(value_type))
                values.append(value)
            return values
        # Traverse one layer deeper and repack list
        else:
            values = []
            for i in range(start_idx, end_idx + 1):
                list_of_tuples = self.get_aggregated_values(objct.children[i], start_time, end_time, resolution, value_type)
                for _tuple in list_of_tuples:
                    values.append(_tuple)
            return values

    def get_raw_values(self, objct, start_time, end_time):
        # type: (MainContainer, datetime, datetime) -> [*(datetime, [*values])]
        """
        Return raw values from a certain date to a certain date in minute resolution
        :param objct: reference to a MainContainer instance "representing kind of the root node"
        :param start_time: datetime object defining the start time, resolution must at least match the desire resolution
        :param end_time: datetime object defining the end time, resolution must at least match the desi re resolution
        :return: list of tuples -> [*(datetime: timestamp, float: value)]
        """
        current_list = objct.children
        if current_list[0] > start_time:
            start_idx = 0
        else:
            start_idx = FileWriter.find_item_in_list(current_list, 0, len(current_list), start_time)
        if current_list[-1] < end_time:
            end_idx = len(current_list) - 1
        else:
            end_idx = FileWriter.find_item_in_list(current_list, 0, len(current_list), end_time)

        # Base case
        if current_list[start_idx].name == "minute":
            values = []
            for i in range(start_idx, end_idx + 1):
                value = (current_list[i].date, current_list[i].children)
                values.append(value)
            return values
        else:
            values = []
            for i in range(start_idx, end_idx + 1):
                list_of_tuples = self.get_raw_values(objct.children[i], start_time, end_time)
                for _tuple in list_of_tuples:
                    values.append(_tuple)
            return values

    def load(self, file_address):
        # type: (str) -> None
        """
        Load previously saved container object
        :param file_address:
        :return:
        """
        self.container = pickle.load(file_address)

'''
new = FileReader("EMFIT_2017-01-05/device-3521-presence-2017-01-05--22.12-07.45-vitals.csv", sensor="hr", date_format="auto")
new.read_file()
w = FileWriter(new.container)
#x = w.get_aggregated_values(w.container["Value Strip"], datetime.datetime(2017, 1, 6, 1, 0),
#                            datetime.datetime(2017, 1, 6, 3, 0), resolution="minute")
#y = w.get_raw_values(w.container["Value Strip"], datetime.datetime(2017, 1, 6, 0, 0), datetime.datetime(2017, 1, 6, 7, 0))
#w.write_csv("test", datetime.datetime(2017, 1, 6, 0, 0), datetime.datetime(2017, 1, 6, 7, 0), "Value Strip")
print(w.write("test.csv", "2017:1:5:0:0", "2017:1:6:6:0", "hr", resolution="minute", output_format="json"))
#print(new.container["Value Strip"].children)
'''




