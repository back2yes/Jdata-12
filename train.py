# -*- coding: utf-8 -*-
"""
Created on Tue Apr 11 19:55:29 2017

@author: Administrator
"""

import numpy as np
import pandas as pd
from datetime import datetime
import matplotlib.pyplot as plt
import seaborn as sns
from collections import Counter

from jdata_util import *
from feature_engineering import *

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

# 读取表
product_table= pd.read_csv(PRODUCT_FILE, header=0, iterator=False)
product_table_id = product_table['sku_id']
comment_table= pd.read_csv(COMMENT_FILE, header=0, iterator=False)

start_date = '2016-02-01'
end_date = '2016-04-15'
action_table = get_action_data(start_date, end_date)
train_start = '2016-03-01'
train_end = '2016-03-31'
comment_feature = get_comment_feature(train_start, train_end, comment_table)
item_feature = get_item_feature(action_table, product_table)
brand_feature=  get_brand_feature(action_table,train_start,train_end)
user_feature = get_user_feature(action_table)



#生成训练数据