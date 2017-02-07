2. Module:
from main import *

# define settings
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

# run script with given settings
parser = ParseController(settings)
parser.main()
