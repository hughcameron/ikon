import pandas as pd
from numpy import nan, var
from os.path import basename, splitext
from icu import CharsetDetector
from glob import glob
import csv

nullables = [
    0,
    "0",
    " ",
    "-",
    ".",
    "01/01/1900 00:00:00",
    "30/12/1899 00:00:00",
    "00:00:00",
    "??:??:??",
]

delimiters = [",", ";", "|", "\t"]

ext_match = {
    ".csv": "csv",
    ".tsv": "csv",
    ".tab": "csv",
    ".dat": "csv",
    ".txt": "csv",
    ".lst": "csv",
    ".xls": "excel",
    ".xlsx": "excel",
}


def detect_encoding(file):
    """
    Uses CharsetDetector from pyicu to determine encoding.
    """
    with open(file, mode="rb") as f:
        data = f.read(1024)
    return CharsetDetector(data).detect().getName()


def non_zero_var(counts):
    if sum(counts) == 0:
        return len(counts)
    else:
        return var(counts)


def string_arg(v):
    if isinstance(v, (int, float)):
        return str(v)
    else:
        return "'" + v + "'"


def get_source_attr(DataFrame):
    """Accepts a DataFrame and returns the source and name attributes used in a DataSource."""
    try:
        name = DataFrame.name
    except AttributeError:
        name = 'df'
    try:
        source = DataFrame.source
    except AttributeError:
        source = 'source'
    return source, name


class DataSource:
    """
    The DataSource object offers a preface to a DataFrame that is derived from a file. 
    The DataSource undergoes a few tests to establish DataFrame attributes.
    """

    def __init__(self, source, name=None, **kwargs):
        self.source = source
        self.kwargs = kwargs

        # Set name based on filename if not provided
        if not name:
            self.name = splitext(basename(source))[0]
        else:
            self.name = name

        # Derive file extension from filename if available
        try:
            self.ext = ext_match[splitext(basename(self.source))[1]]
        except KeyError:
            self.ext = None

        # Infer enconding
        try:
            self.encoding = kwargs.pop("encoding")
        except KeyError:
            try:
                self.encoding = detect_encoding(self.source)
            except FileNotFoundError:
                self.encoding = None

        # Infer delimiteter by using csv Sniffer or by evaluating
        # minumum varience of delimiter occurence in first 10 lines
        if self.ext == 'excel':
            self.delimiter = '\t'
        else:
            try:
                self.delimiter = kwargs.pop("sep")
            except KeyError:
                try:
                    with open(self.source, "r", encoding=self.encoding) as f:
                        try:
                            lines = f.readline() + "\n" + f.readline()
                            dialect = csv.Sniffer().sniff(
                                lines, delimiters=",;|\t")
                            self.delimiter = dialect.delimiter
                        except:
                            lines = [f.readline() for i in range(10)]
                            counts = [[l.count(d) for l in lines]
                                    for d in delimiters]
                            varience = [non_zero_var(c) for c in counts]
                            self.delimiter = delimiters[varience.index(
                                min(varience))]
                except FileNotFoundError:
                    self.delimiter = None

    def df(self):
        """Generate a DataFrame from a Datasource"""
        if self.ext == "csv":
            df = pd.read_csv(
                self.source, sep=self.delimiter, encoding=self.encoding, **self.kwargs)
            return df
        else:
            # TODO provide multi tab method for dataframes
            xl = pd.ExcelFile(self.source)
            xl.sheet_names
            df = xl.parse(xl.sheet_names[0])
            return df

    def statement(self):
        """Return a string that can be run to generate DataFrames."""
        define = "{0} = pd.read_{1}('{2}', encoding='{3}', sep='{4}'".format(
            self.name, self.ext, self.source, self.encoding, self.delimiter)
        arguments = [
            ", " + k + "=" + string_arg(v) for k, v in self.kwargs.items()
        ]
        arguments = "".join(arguments)
        statement = define + arguments + ")"
        return statement

def summary(data, **kwargs):
    """Generate a summary from a DataSource or DataFrame"""
    if type(data) == DataSource:
        ds = data
        df = ds.df()
    elif type(data) == pd.DataFrame:
        ds = DataSource(*get_source_attr(data))
        df = data
        # TODO provide warning if df.name and df.source are set to defaults across multiple frames
    else:
        raise ValueError(
            "Expecting type DataSource or DataFrame, received {}.".format(
                type(data)))
    na_values = kwargs.get("na_values", [])
    na_values = nullables + na_values
    nulled = df.apply(
        lambda x: round(sum(x.isin(na_values)) / len(df), 2), axis=0)
    df = df.replace(na_values, nan)
    s = pd.DataFrame()
    s["type"] = df.dtypes
    s["count"] = df.count()
    s["length"] = len(df)
    s["coverage"] = round((s["count"] / len(df)), 2)
    s["cardinality"] = df.nunique()
    s["nulled"] = nulled
    s["mode coverage"] = df.apply(
        lambda x: round(x.value_counts().max() / len(x), 2), axis=0)
    s["mode"] = df.mode().head(1).T
    s["sample"] = df.sample().T
    s.index.name = "column"
    s.reset_index(inplace=True)
    s["reference"] = ds.name + "['" + s["column"] + "']"
    s["file"] = ds.source
    s.index += 1
    s.index.name = "sequence"
    return s

def sources(path, recursive=False, **kwargs):
    fileset = glob(path, recursive=recursive)
    sources = []
    for f in fileset:
        s = DataSource(f, **kwargs)
        sources.append(s)
    return sources

def statements(path, recursive=False, source_attr=False, **kwargs):
    fileset = glob(path, recursive=recursive)
    statement_list = []
    frame_list = []
    for f in fileset:
        s = DataSource(f, **kwargs)
        statement_list.append(s.statement())
        if source_attr:
            statement_list.append("{}.name = '{}'".format(s.name, s.name))
            statement_list.append("{}.source = '{}'".format(s.name, s.source))
        else:
            pass
        frame_list.append(s.name)
    statements = "\n".join(statement_list)
    statements += "\n\ndf_list = " + str(frame_list).replace("'", "")
    return statements

def summaries(group, recursive=False, **kwargs):
    """Generate a Summary for a group of files, DataSources or DataFrames."""
    data_list = []
    if type(group) == list:
        for g in group:
            if type(g) == str:
                d = sources(g, recursive=recursive, **kwargs)
                data_list.append(d)
            elif type(g) == DataSource:
                data_list.append(g)
            elif type(g) == pd.DataFrame:
                data_list.append(g)
            else:
                raise ValueError(
                    "Expecting type list, path or DataSource, received {}.".
                    format(type(g)))
    elif type(group) == str:
        d = sources(group, recursive=recursive, **kwargs)
        data_list += d
    elif type(group) == DataSource:
        data_list.append(group)
    else:
        raise ValueError("Expecting type list, path or DataSource.")
    summary_list = [summary(d, **kwargs) for d in data_list]
    return pd.concat(summary_list, axis=0)