import pandas as pd


def data_report(df):
    report = pd.DataFrame()
    report['count'] = df.count()
    report['length'] = len(df)
    report['coverage'] = round((report['count'] / len(df)), 2)
    report['cardinality'] = df.nunique()
    report['type'] = df.dtypes
    report['mode'] = df.mode().head(1).T
    report['sample'] = df.sample().T
    report.index.name = 'column'
    return report
