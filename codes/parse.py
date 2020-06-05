# print("Starting job {}".format(load_job.job_id))

import json 
import csv
import pprint

# これで一つのデータの情報をチェックできる
# with open("../data/part_v003_o001_r_00000.json") as f:
#     lines = f.readlines()
#     print('read lines')
# f.close()
# print('closed')
# pprint.pprint(lines[1]) # str


import ijson

# with open('../data/part_v003_o001_r_00000.json', 'r') as file:
#     dates = ijson.items(file, 'created_at.item', multiple_values=True)
#     tweets = ijson.items(file, 'text', multiple_values=True)
#     # for data, tweet in zip(dates, tweets):
#     #     print(data, tweet)
#     # for date in dates:
#     #     print(date)
#     print(dates)
#     # print(len(dates))


# with open('../data/part_v003_o001_r_00000.json', 'r') as file:
#     ijson_generator = ijson.items(file, "created_at.text", multiple_values=True)
#     key2_value = next(ijson_generator)
#     print(key2_value)


# def load_json(filename):
#     with open(filename, 'r') as fd:
#         parser = ijson.parse(fd, multiple_values=True)
#         ret = {'dates': {}}
#         for prefix, event, value in parser:
#             if (prefix, event) == ('created_at', 'map_key'):
#                 date = value
#                 ret['dates'][date] = {}
#             # elif prefix.endswith('.shortname'):
#             #     ret['builders'][buildername]['shortname'] = value

#         return ret

# if __name__ == "__main__":
#     dates = load_json('../data/part_v003_o001_r_00000.json')
#     print(dates




from datetime import datetime


data = ijson.parse(open('../data/part_v003_o001_r_00000.json', 'r'), multiple_values=True)

l = []
for prefix, event, value in data:
    if prefix == 'created_at':
    	l.append(datetime.strptime(value, '%a %b %d %H:%M:%S +0000 %Y'))
    	print(l)
    # if prefix == 'text':
    # 	a = value
    # 	print(a)


    # if prefix == 'text':
    # 	text = value
    # print(date, text)

