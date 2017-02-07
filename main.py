from Parser import *
import sys, getopt

"""Define Settings"""
settings = {
    "-i": "required",  # path to input file
    "-o": "required",  # path to output file
    "-c": None,  # column name of the column to be extracted (header), defaults to all, however only numerical values can be extracted
    "--start": "1980:10:10:10:10",  # start point as date and time "yyyy:mm:dd:hh:mm", defaults to whole file
    "--end": "2050:10:10:10:10",  # end point as date and time "yyyy:mm:dd:hh:mm", defaults to whole file
    "--sep": ",",  # separator used in the input csv file (same will be used for output)
    "--format_out": "csv",  # desired output format "csv" or "json"
    "--date_format": "auto",  # date format of the input file timestamp (only US needs to be specified as "US" since it's ambiguous)
    "--resolution": "minute",  # desired resolution of output (if aggregated) -> "day", "hour", "minute"
    "--aggregation_type": "none",  # how values should be aggregated "mean", "min", "max", "none" - "none" will just output raw values at 1 minute resolution
    "--timestamp_column": -1,  # csv field where timestamp is located (starting at 0), -1 = automatically detect it - optional
    "--hour_format": "auto",  # 12 or 24 hour format as int 12 or 24, defaults to automatic detection - optional
    "--sensor_name": ""  # name of sensor - optional
}


class ParseController(object):

    def __init__(self, settings):
        self.settings = settings
        self.file_reader = None
        self.file_writer = None

    def __str__(self):
        return str(settings)

    def main(self):
        self.file_reader = FileReader(self.settings["-i"], self.settings["--timestamp_column"],
                                      self.settings["--sep"], self.settings["--sensor_name"],
                                      self.settings["-c"], self.settings["--hour_format"],
                                      self.settings["--date_format"])
        self.file_reader.read_file()
        self.file_writer = FileWriter(self.file_reader.container)
        self.file_writer.write(self.settings["-o"], self.settings["--start"], self.settings["--end"],
                               self.settings["-c"], self.settings["--sep"], self.settings["--resolution"],
                               self.settings["--aggregation_type"], self.settings["--format_out"])


def cli(argv):
    param_list = ["-i", "-o", "-c", "--sep", "--start", "--end", "--format_out", "--date_format", "--resolution",
                  "--aggregation_type", "--timestamp_column", "--sensor_name", "--hour_format"]

    usage = "parse.py -i <inputfile> -o <outputfile> -c <colname> \n\n" \
            "for information about additional parameters type parser.py -h"

    _help = "Usage example: parse.py -i <inputfile> -o <outputfile> -c <colname> --<additional parameter>=<parameter> \n\n" \
           "-i <inputfile> \n\n" \
           "-o <outputfile> \n\n" \
           "-c <colname>     name of the column to be extracted - thus header name of the respective field \n\n" \
           "--sep=<separator>     separator of the input csv file \n\n" \
           "--start=<start_time>     where extraction should begin 'yyyy:mm:dd:hh:mm' -> e.g. '2017:01:01:14:15' \n\n" \
           "--end=<end_time>     where extraction should end 'yyyy:mm:dd:hh:mm' -> e.g. '2017:01:01:14:15' \n\n" \
           "--format_out=<file_format>     desired output file formatting, 'csv' or 'json' \n\n" \
           "--resolution=<time_resolution>     desired output resolution of time -> 'day', 'hour' or 'minute' \n\n" \
           "--aggregation_type=<type>     whether values should be aggregated and how 'none', 'mean', 'min', 'max' \n\n" \
           "--timestamp_column=<number>     in which field the timestamp can be found (should usually be automatically)\n\n" \
           "--date_format=<format>     what timeformat is used in the inputfile, only specify if it's american format -> 'US'\n\n" \
           "--sensor_name=<name>     optional name of the used sensor e.g. 'ECG'\n\n" \
           "--hour_format=<number>    hour format used in input file 12 or 24, is by default detected automatically\n"
    try:
        opts, args = getopt.getopt(argv, "hi:o:c:", ["sep=", "start=", "end=", "format_out=", "date_format=",
                                                     "resolution=", "aggregation_type=", "timestamp_column=",
                                                     "sensor_name=", "hour_format="])
    except getopt.GetoptError as e:
        print(e)
        print(usage)
        sys.exit(2)

    for opt, arg in opts:
        if opt == "-h":
            print(_help)
            sys.exit(0)
        for param in param_list:
            if opt in param:
                settings[param] = arg
    settings["--timestamp_column"] = int(settings["--timestamp_column"])


if __name__ == "__main__":
    if len(sys.argv) > 1:
        cli(sys.argv[1:])
        print("\n" + str(settings))
        parser = ParseController(settings)
        parser.main()

