import pandas as pd
from numpy import nan
from os.path import basename, splitext

nullables = [
    0, '0', ' ', '-', '.', '01/01/1900 00:00:00', '30/12/1899 00:00:00',
    '00:00:00', '??:??:??'
]


def nulled_counts(series, na_values):
    counts = {}
    for n in na_values:
        counts['n'] = sum(series.isin([n]))
    counts = {k: v for k, v in counts.items() if v=0}
    counts = [(k, v) for k, v in counts.items()]
    return counts


def series_summary(source_data):
    summaries = []
    for d in source_data:
        df = pd.read_csv(d.get('file'), **d.get('kwargs', {'sep': ','}))
        na_values = nullables + d.get('na_values', [])
        nulled = df.apply(lambda x: sum(x.isin(na_values)), axis=0)
        df = df.replace(na_values, nan)
        s = pd.DataFrame()
        s['type'] = df.dtypes
        s['count'] = df.count()
        s['length'] = len(df)
        s['nulled'] = df.apply(nulled_counts, args=(na_values,), axis=0)
        s['nulled sum'] = nulled
        s['coverage'] = round((s['count'] / len(df)), 2)
        s['cardinality'] = df.nunique()
        s['mode coverage'] = df.apply(
            lambda x: round(x.value_counts().max() / len(x), 2), axis=0)
        s['mode'] = df.mode().head(1).T
        s['sample'] = df.sample().T
        s.index.name = 'column'
        s.reset_index(inplace=True)
        df_name = d.get('df_name', splitext(basename(d.get('file')))[0])
        s['reference'] = df_name + "['" + s['column'] + "']"
        s['file'] = d.get('file')
        s.index += 1
        s.index.name = 'sequence'
    summaries.append(s)
    summaries = pd.concat(summaries, axis=0)
    return summaries
