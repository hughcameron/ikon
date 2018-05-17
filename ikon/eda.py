import pandas as pd
from numpy import nan


def data_report(df, na_values=[]):
    void = df.apply(lambda x: len(x[x.isin(na_values)]), axis=0)
    df = df.replace(na_values, nan)
    report = pd.DataFrame()
    report['type'] = df.dtypes
    report['count'] = df.count()
    report['length'] = len(df)
    report['void'] = void
    report['coverage'] = round((report['count'] / len(df)), 2)
    report['cardinality'] = df.nunique()
    report['mode coverage'] = df.apply(
        lambda x: round(x.value_counts().max() / len(x), 2), axis=0)
    report['mode'] = df.mode().head(1).T
    report['sample'] = df.sample().T
    report.index.name = 'column'
    report.reset_index(inplace=True)
    report.index += 1
    report.index.name = 'sequence'
    return report
