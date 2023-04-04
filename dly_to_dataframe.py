
import re
import os
import sys
import glob
import numpy as np
import pandas as pd
from tqdm import tqdm


__all__ = ['parse_line', 'month_to_df', 'read_dly_file']


def parse_line(line):
    ID = line[:11]
    year = int(line[11:15])
    month = int(line[15:17])
    element = line[17:21]
    start_col = 21
    value_len = 5
    flags_len = 3
    values, mflags, qflags, sflags = [], [], [], []
    for day in range(31):
        a = start_col + day * (value_len + flags_len)
        b = a + value_len
        c = b + flags_len
        values.append(int(line[a : b]))
        mflags.append(line[b   : b+1])
        qflags.append(line[b+1 : b+2])
        sflags.append(line[b+2 : b+3])
    return ID, year, month, element, np.array(values, dtype=float), mflags, qflags, sflags


def month_to_df(year, month, element, values):
    days_in_a_month = {1:31, 2:28, 3:31, 4:30, 5:31, 6:30, 7:31, 8:31, 9:30, 10:31, 11:30, 12:31}
    coeff = {'TAVG': 0.1, 'TMAX': 0.1, 'TMIN': 0.1, 'PRCP': 0.1, 'SNOW': 1., 'SNWD': 1.}
    try:
        X = np.array(values[:days_in_a_month[month]]) * coeff[element]
        df = pd.DataFrame(data={'year': year + np.zeros(X.shape, dtype=int),
                                'month':  month + np.zeros(X.shape, dtype=int),
                                'day': 1 + np.arange(days_in_a_month[month], dtype=int)})
        df = pd.DataFrame(data={element.lower(): X}, index=pd.to_datetime(df))
    except:
        df = None
    return df


def read_dly_file(in_file, min_year=1900, max_year=2100):
    data = {}
    with open(in_file) as fid:
        for line in fid:
            ID,year,month,element,values,mflags,qflags,sflags = parse_line(line)
            values[values == -9999] = np.nan
            if year >= min_year and year <= max_year:
                df = month_to_df(year, month, element, values)
                if df is not None:
                    if element not in data:
                        data[element] = []
                    data[element].append(df)
    dfs = [pd.concat(d) for d in data.values()]
    df = dfs[0]
    for right in dfs[1:]:
        df = df.join(right)
    df['year'] = [d.year for d in df.index]
    df['month'] = [d.month for d in df.index]
    df['day'] = [d.day for d in df.index]
    cols = df.columns.to_list()
    return df[cols[-3:] + cols[:-3]]


progname = os.path.basename(sys.argv[0])


def usage(exit_code=None):
    print(f'{progname}: [-d | --dly-dir <dir>] [-r | --regex] [--min-year <year>] [--max-year <year>] [-f | --force] pattern')
    if exit_code is not None:
        sys.exit(exit_code)

    
if __name__ == '__main__':

    if len(sys.argv) == 1 or sys.argv[1] in ('-h', '--help'):
        usage(0)

    is_regex = False
    dly_dir = ''
    min_year, max_year = 1900, 2100
    force = False
    n_args = len(sys.argv)
    i = 1
    
    while i < n_args:
        arg = sys.argv[i]
        if sys.argv[i] in ('-r','--regex'):
            is_regex = True
        elif sys.argv[i] in ('-d', '--dly-dir'):
            i += 1
            dly_dir = sys.argv[i]
        elif sys.argv[i] == '--min-year':
            i += 1
            min_year = int(sys.argv[i])
        elif sys.argv[i] == '--max-year':
            i += 1
            max_year = int(sys.argv[i])
        elif sys.argv[i] in ('-f', '--force'):
            force = True
        elif sys.argv[i][0] == '-':
            print(f'{progname}: {sys.argv[i]}: unknown option')
            sys.exit(1)
        else:
            break
        i += 1

    if i == n_args:
        usage(2)
        
    if dly_dir != '' and not os.path.isdir(dly_dir):
        print(f'{progname}: {dly_dir}: no such directory.')
        sys.exit(3)

    if not is_regex:
        dly_files = [os.path.join(dly_dir, sys.argv[i])]
        if os.path.splitext(dly_files[0])[1] != '.dly':
            dly_files[0] += '.dly'
    else:
        all_dly_files = sorted(glob.glob(os.path.join(dly_dir, '*.dly')))
        dly_files = [f for f in all_dly_files if re.search(sys.argv[i], os.path.basename(f))]

    for dly_file in tqdm(dly_files, ascii=True, ncols=100):
        out_file = os.path.join(os.path.dirname(dly_file),
                                os.path.splitext(os.path.basename(dly_file))[0] + '.parquet.gz')
        if not os.path.isfile(out_file) or force:
            df = read_dly_file(dly_file, min_year, max_year)
            df.to_parquet(out_file, compression='gzip')
    
