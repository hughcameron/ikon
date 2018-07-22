import pandas as pd
from numpy import nan, var
from os.path import basename, splitext
from chardet.universaldetector import UniversalDetector
from glob import glob
import csv

nullables = [
    0, '0', ' ', '-', '.', '01/01/1900 00:00:00', '30/12/1899 00:00:00',
    '00:00:00', '??:??:??'
]

delimiters = [',', ';', '|', '\t']

ext_match = {
    '.csv': 'csv',
    '.tsv': 'csv',
    '.tab': 'csv',
    '.dat': 'csv',
    '.txt': 'csv',
    '.lst': 'csv',
    '.xls': 'excel',
    '.xlsx': 'excel'
}

preferred_encodings = ['utf-8', 'ascii', 'latin1']


def detect_encoding(file):
    """
    The UniversalDetector is resource intensive. This is only called
    when infer_encoding fails to resolve a preferred encoding.
    """
    with open(file, mode='rb') as f:
        detector = UniversalDetector()
        for line in f.readlines():
            detector.feed(line)
            if detector.done: break
        detector.close()
    return detector.result["encoding"]


def infer_encoding(file):
    """Test for encoding with preferred_encodings or UniversalDetector."""
    with open(file, 'rb') as f:
        data = f.read(1024)
    i = 0
    while i < len(preferred_encodings):
        try:
            data.decode(preferred_encodings[i])
        except UnicodeDecodeError:
            i += 1
        else:
            return preferred_encodings[i]
    else:
        return detect_encoding(file)


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


class DataSource:
    '''
    The DataSource object offers a preface to a DataFrame that is derived from a file. 
    The DataSource undergoes a few tests to establish  attributes useful for dealing with large groups of files.
    '''

    def __init__(self, source, name=None, **kwargs):
        self.source = source
        self.kwargs = kwargs

        #Set name based on filename if not provided
        if not name:
            self.name = splitext(basename(source))[0]
        else:
            self.name = name

        #Derive file extension from filename
        self.ext = ext_match[splitext(basename(self.source))[1]]

        #Infer enconding
        try:
            self.encoding = kwargs.pop('encoding')
        except KeyError:
            self.encoding = infer_encoding(self.source)

        #Infer delimiteter by using csv Sniffer or by evaluating
        # minumum varience of delimiter occurence in first 10 lines
        try:
            self.delimiter = kwargs.pop('sep')
        except KeyError:
            with open(self.source, 'r', encoding=self.encoding) as f:
                try:
                    lines = f.readline() + '\n' + f.readline()
                    dialect = csv.Sniffer().sniff(lines, delimiters=',;|\t')
                    self.delimiter = dialect.delimiter
                except:
                    lines = [f.readline() for i in range(10)]
                    counts = [[l.count(d) for l in lines] for d in delimiters]
                    varience = [non_zero_var(c) for c in counts]
                    self.delimiter = delimiters[varience.index(min(varience))]


    def statement(self):
        """Return a string that can be run to generate DataFrames."""
        define = "{0} = pd.read_{1}('{2}', encoding='{3}', sep='{4}'".format(
            self.name, self.ext, self.source, self.encoding, self.delimiter)
        arguments = [
            ', ' + k + '=' + string_arg(v) for k, v in self.kwargs.items()
        ]
        arguments = ''.join(arguments)
        statement = define + arguments + ')'
        return statement


def find_sources(path, recursive=False, **kwargs):
    source_list = []
    group = glob(path, recursive=recursive)
    for g in group:
        source_list.append(DataSource(g, **kwargs))
    return source_list


def gen_dataframe(DataSource):
    """Generate a DataFrame from a Datasource"""
    ds = DataSource
    if ds.ext == 'csv':
        ds.df = pd.read_csv(
            ds.source, sep=ds.delimiter, encoding=ds.encoding, **ds.kwargs)
        return ds.df
    else:
        ds.df = pd.read_excel(ds.source)
        return ds.df


def read_source(path, recursive=False, **kwargs):
    """Return one or many files as DataFrames"""
    fetch = glob(path, recursive=recursive)
    if len(fetch) == 1:
        ds = DataSource(path, **kwargs)
        return gen_dataframe(ds)
    else:
        dataframes = []
        for f in fetch:
            ds = DataSource(f, **kwargs)
            dataframes.append(gen_dataframe(ds))
        return dataframes


def frame_summary(DataSource, **kwargs):
    """Generate Series Summary from DataSource"""
    df = gen_dataframe(DataSource)
    na_values = kwargs.get('na_values', [])
    na_values = nullables + na_values
    nulled = df.apply(
        lambda x: round(sum(x.isin(na_values)) / len(df), 2), axis=0)
    df = df.replace(na_values, nan)
    s = pd.DataFrame()
    s['type'] = df.dtypes
    s['count'] = df.count()
    s['length'] = len(df)
    s['coverage'] = round((s['count'] / len(df)), 2)
    s['cardinality'] = df.nunique()
    s['nulled'] = nulled
    s['mode coverage'] = df.apply(
        lambda x: round(x.value_counts().max() / len(x), 2), axis=0)
    s['mode'] = df.mode().head(1).T
    s['sample'] = df.sample().T
    s.index.name = 'column'
    s.reset_index(inplace=True)
    s['reference'] = DataSource.name + "['" + s['column'] + "']"
    s['file'] = DataSource.source
    s.index += 1
    s.index.name = 'sequence'
    return s


def data_summary(group, recursive=False, **kwargs):
    """Generate a Frame Summary for a group of paths or DataSources"""
    datasource_list = []
    if type(group) == list:
        for g in group:
            if type(g) == str:
                ds = find_sources(g, recursive=recursive, **kwargs)
                datasource_list += ds
            elif type(g) == DataSource:
                datasource_list.append(g)
            else:
                raise ValueError('Expecting type list, path or DataSource.')
    elif type(group) == str:
        ds = find_sources(group, recursive=recursive, **kwargs)
        datasource_list += ds
    elif type(group) == DataSource:
        datasource_list.append(group)
    else:
        raise ValueError('Expecting type list, path or DataSource.')
    summary_list = [frame_summary(ds, **kwargs) for ds in datasource_list]
    return pd.concat(summary_list, axis=0)


def statements(path, recursive=False, **kwargs):
    fileset = glob(path, recursive=recursive)
    statement_list = []
    frame_list = []
    for f in fileset:
        s = DataSource(f, **kwargs)
        statement_list.append(s.statement())
        frame_list.append(s.name)
    statements = '\n'.join(statement_list)
    statements += '\n\ndf_list = ' + str(frame_list).replace("'", "")
    return statements