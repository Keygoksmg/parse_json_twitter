import csv
from datetime import datetime
import numpy as np
import pandas as pd
import time
from datetime import datetime
import os
from os import path

import ijson

# Import JSON
jsons = ['../data/part_v003_o001_r_00000.json', '../data/part_v003_o001_r_00001.json']
def json2csv(jsons):
    # jsons = ['../data/part_v003_o001_r_00001.json']
    jsons = ['../data/part_v003_o001_r_00000.json', '../data/part_v003_o001_r_00001.json']
    dfrts = []
    dfors = []
    for json in jsons:

        dates = []
        tweets = []
        start = time.time()
        with open(json, 'r') as file:
            pet_parse = ijson.parse(file, multiple_values=True)
            for prefix, event, value in pet_parse:
                #         print('prefix:{}, event:{}, value:{}'.format(prefix, event, value))
                if prefix == 'created_at':
                    dates.append(datetime.strptime(value, '%a %b %d %H:%M:%S +0000 %Y'))
                if prefix == 'text':
                    tweets.append(value)
        print("elapsed_time:{0}".format(time.time() - start) + "[sec]")

        datescopy, twt = dates.copy(), tweets.copy()

        data = np.vstack([datescopy, twt]).T

        df = pd.DataFrame(data, columns=['date', 'tweet'])
        df['year'] = df['date'].dt.year
        df['month'] = df['date'].dt.month
        df['day'] = df['date'].dt.day
        df['hour'] = df['date'].dt.hour
        df['date'] = df['date'].dt.date

        # RT_flag: True if tweet containt 'RT', otherwise False
        df['RT_flag'] = df['tweet'].str.contains('RT')

        # df

        # divide df into one with and without RT
        dfrt = df[df['RT_flag'] == True]
        dfor = df[df['RT_flag'] == False]

        dfors.append(dfor)
        dfrts.append(dfrt)

    return dfrt, dfor

csvfile = '../data/WordTimeSeries.csv'
def keywords(csvfile, filetype):
    '''
    filetypes: words' meta kind
    filenames: columns correspended to keyword e.g. ['C1', 'C2',..'C35']
    words_x: words dataframe of x(= C,D,...)

    :param csvfile:
    :return: dfw_x, filenames
    '''

    dfw = pd.read_csv(csvfile, encoding='utf-8').rename(columns={'Unnamed: 0': 'types',
                                                                                      'file name': 'file_name',
                                                                                      'orignal form': 'orignal_form',
                                                                                      'English translation': 'English_translation'})
    # Drop nan and False in order to use query
    booleanDictionary = {True: 'TRUE', False: 'FALSE'}
    dfw = dfw.replace(booleanDictionary)
    dfw = dfw.dropna(how='all')

    filetypes = ['T', 'D', 'A', 'V', 'F', 'C']
    filetype = filetype
    filenames = [filename for filename in dfw.file_name.tolist() if filetype in filename]

    xd = {}
    xd['T'] = 'file_name.str.contains("T")'
    xd['D'] = 'file_name.str.contains("D")'
    xd['A'] = 'file_name.str.contains("A")'
    xd['V'] = 'file_name.str.contains("V")'
    xd['F'] = 'file_name.str.contains("F")'
    xd['C'] = 'file_name.str.contains("C")'

    dfw_x = dfw.query('file_name.str.contains("C")', engine='python')


    return dfw_x, filenames

## Find tweets which conatins keywords using dfor and words
def findtwt(df, dfw_x, filenames):
    '''

    :param df:
    :param dfw_x:
    :param filenames:
    :return: df_x:
    '''
    keywords_jp = dfw_x.orignal_form.tolist()
    df_x = df[df['tweet'].str.contains('|'.join(keywords_jp))]

    # Add columns(['C1'], ['C2'],...) to dfor_c
    for col, key in zip(filenames, keywords_jp):
        l = []
        for row in df_x.itertuples():
            if key in row.tweet:
                l.append(1)
            else:
                l.append(0)
        df_x[col] = l

    # save to csv
    # df_x.to_csv('x.csv')
    return df_x

def date_data(df_x, filenames):
    uni_dates = df_x['date'].tolist()
    uni_dates = sorted(set(uni_dates), key=uni_dates.index)

    # Create rows
    rows = []
    for date in uni_dates:
        d = {}
        d['date'] = str(date)
        for col in filenames:
            _df = df_x.groupby('date').get_group(date)
            d[col] = _df[col].sum()
        rows.append(d)

    # Create columns
    cols = filenames.copy()
    cols.insert(0, 'date')

    dft = pd.DataFrame(columns=cols)
    for row in rows:
        dft = dft.append(row, ignore_index=True)

    return dft

# dfs = [df1, df2, df3, ..., df20]
def concat_dfs(dfs):
    for i, df in enumerate(dfs):
        if i == 0:
            dfnew = df
        else:
            dfnew = pd.concat([dfnew, df], axis=0)

    return dfnew

if __name__ == '__main__':
    jsons_files = []
    json_file = '../data/part_v003_o001_r_00000.json'
    dfs_or = []
    for json_file in jsons_files:
        # json to csv
        dfrt, dfor = json2csv(json_file)
        dfs_or.append(dfor)

    for filetype in filetypes:
        # load keywords
        csvfile = '../data/WordTimeSeries.csv'
        dfw_x, filenames = keywords(csvfile, filetype)

        # find match tweets
        df_x = findtwt(dfor, dfw_x, filenames)

        # results
        dfs = []
        df = date_data(df_x, filenames)
        dfs.append(df)

    # connect all data for 1 data
    df_result = concat_dfs(dfs)

