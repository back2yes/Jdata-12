# -*- coding: utf-8 -*-
"""
Created on Sat Apr 08 20:16:50 2017

@author: Administrator
"""
import numpy as np
import pandas as pd
import datetime
import matplotlib.pyplot as plt
import seaborn as sns
from wordcloud import WordCloud
from collections import Counter

from jdata_util import *

# 2-4月的行为数据
ACTION_201602_FILE = "H:/dataset/JData/JData_Action_201602.csv"
ACTION_201603_FILE = "H:/dataset/JData/JData_Action_201603.csv"
ACTION_201604_FILE = "H:/dataset/JData/JData_Action_201604.csv"
# 评论，商品，用户数据
COMMENT_FILE = "H:/dataset/JData/JData_Comment.csv"
PRODUCT_FILE = "H:/dataset/JData/JData_Product.csv"
NEW_USER_FILE = "H:/dataset/JData/JData_User.csv"
USER_TABLE_FILE = "H:/dataset/JData/user_table.csv"
ITEM_TABLE_FILE = "H:/dataset/JData/item_table.csv"
# 预测结果
PRED_FILE = "H:/dataset/JData/pred/pred_user_item.csv"

def get_current_time():
    return datetime.datetime.now().strftime('%Y-%m-%d-%H:%M')
def get_filename_with_time(filename):
    filename_split = filename.split('.')
    return filename_split[0]+get_current_time()+'.csv'
##随机写一些用户数据
num_sample = 10000
file_list = [ACTION_201602_FILE, 
             ACTION_201603_FILE,
             ACTION_201604_FILE,
             NEW_USER_FILE,
             COMMENT_FILE]

for fname in file_list:
    with open(fname, 'rb') as fi:
        with open("H:/dataset/JData/sample/"+fname.split("/")[-1], 'wb') as fo:
            for i in range(num_sample):
                fo.write(fi.readline())
                
df_usr = pd.read_csv(USER_TABLE_FILE, header=0,encoding='gbk')
# 输出前5行数据
df_usr.head(5)              

##读取item列表
##20000个商品
product_table= pd.read_csv(PRODUCT_FILE, header=0, iterator=False)
product_table_id = product_table['sku_id']
comment_table= pd.read_csv(COMMENT_FILE, header=0, iterator=False)
##读取4月份的信息
df_April= pd.read_csv(ACTION_201604_FILE, header=0, iterator=False)
##df_April['time'] =  pd.to_datetime(df_April['time'], format='%Y-%m-%d %H:%M:%S')
#当天一共有96万条记录
df_April_15 = df_April[df_April['time'] >'2016-04-15']
###查看4月15日的统计情况
operation_type=['view','add_cart','delete_cart',
                'buy','concern','click']
for i in range(1,7):
    print(operation_type[i-1],':',len(df_April_15[df_April_15['type'] == i]))


###简单规则 
def key(x):
    add_cart = False
    delete_cart = False
    buy = False
    concern = False
    for val in x:
        if val == 2:
            add_cart = True
        if val ==3:
            delete_cart = True
        if val == 4:
            buy = True
        if val == 5:
            concern = True
    return (add_cart or concern) and (not  delete_cart) and (not buy)
key2 = lambda x:(len(x)>5)
##取出当天记录中加购物车或者关注，但是没有买或删除的
df_pred = df_April_15.groupby(['user_id','sku_id'], as_index=False).agg({'type':key})
df_pred = df_pred[df_pred['type']==True]##8215条

#只选择需要预测的商品，3795条
sku_id = product_table['sku_id'].tolist()
sku_id = set(sku_id)
criterion = df_pred['sku_id'].map(lambda x: x in sku_id)
df_new_pred =df_pred[criterion]

def write_to_file_without_duplicates(df_pred, filename):
    print("number of results before removing duplicates:",len(df_pred))
    df_pred = df_pred.drop_duplicates('user_id',keep='last')
    print("number of results before removing duplicates:",len(df_pred))

    df_pred['user_id'] = df_pred['user_id'].astype(int)
    df_pred= df_pred[['user_id','sku_id']]
    df_pred.to_csv(filename, index=False)

    # 写入文件
write_to_file_without_duplicates(df_new_pred, 'pred20170409')

#%% 用户分析
#本部分分析总共有
##df_pair_recall = pd.DataFrame(columns=['view','add_cart','delete_cart','buy','concern','click','any_action'])
pair_recall_list = []
user_recall_list = []

for day in range(2,16):
    pair_recall, user_recall = get_backward_rate_info(df_April, month =4,day = day)
    pair_recall_list.append(pair_recall)
    user_recall_list.append(user_recall)

df_pair_recall = pd.DataFrame(pair_recall_list,
                              columns=['view','add_cart','delete_cart','buy','concern','click','any_action'])
df_user_recall = pd.DataFrame(user_recall_list,
                              columns=['view','add_cart','delete_cart','buy','concern','click','any_action'])

pair_precision_list = []
user_precision_list = []

for day in range(1,11):
    pair_precision, user_precision = get_forward_rate_info(df_April, 4, day, sku_id)
    pair_precision_list.append( pair_precision)
    user_precision_list.append(user_precision)

df_pair_precision = pd.DataFrame(pair_precision_list,
                              columns=['view','add_cart','delete_cart','buy','concern','click'])
df_user_precision = pd.DataFrame(user_precision_list,
                              columns=['view','add_cart','delete_cart','buy','concern','click'])


sns.tsplot(data=df_pair_recall)

#%% 商品分析
category_dis, brand_dis = get_item_statistic(product_table)

