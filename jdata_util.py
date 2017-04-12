# -*- coding: utf-8 -*-
"""
Created on Mon Apr 10 09:57:22 2017

@author: Administrator
"""

import pickle
import os
import numpy as np
import pandas as pd
# %%常量定义

# 初始文件存放路径
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

# pkl文件存放路径
ACTIOIN_PKL_PATH = 'H:/dataset/JData/cache/pkl/'
USER_FEATURE_PATH = 'H:/dataset/JData/cache/user_feature/'
ITEM_FEATURE_PATH = 'H:/dataset/JData/cache/item_feature/'
UI_FEATURE_PATH = 'H:/dataset/JData/cache/ui_feature/'

VIEW_ACTION = 1
ADD_CART_ACTION = 2
DELETE_CART_ACTION = 3
BUY_ACTION = 4
CONCERN_ACTION = 5
CLICK_ACTION = 6
action_list = [VIEW_ACTION,
               ADD_CART_ACTION,
               DELETE_CART_ACTION,
               BUY_ACTION,
               CONCERN_ACTION,
               CLICK_ACTION]
               
action_str = ['view','add_cart','delete_cart','buy','concern','click']


def get_score(pred_df, true_df):
    return None
    
def get_number_format(number):
    if number < 10:
        return '0'+str(number)
    else:
        return str(number)
        
def get_date_format(month, day):
    return ("2016-%s-%s" % (get_number_format(month),get_number_format(day)))
    
def get_data_in_date(df, start_month, start_day, end_month = -1, end_day = -1, action =-1):
    """
    返回给定日期间隔的数据，如果只给定某一天那么就返回当天
    """
    start_date = get_date_format(start_month, start_day)
    if end_month == -1:
        end_date = get_date_format(start_month, start_day + 1)
    else:
        end_date = get_date_format(end_month, end_day + 1)

    return df[(df['time'] >=start_date) & (df['time'] <end_date)]
    #return df[(df['time'] >=start_date)]
    
def get_data_from_date(df, start_date, end_date):
    return df[(df.time >= start_date)&(df.time<end_date)]
              
def get_action_data(start_date, end_date):
    """
    返回start_date到end_date
    """
    
    dump_path = ACTIOIN_PKL_PATH + 'all_action_%s_%s.pkl' % (start_date, end_date)
    if os.path.exists(dump_path):
        actions = pickle.load(open(dump_path, 'rb'))
    else:
        action_1 = pd.read_csv(ACTION_201602_FILE, header=0, iterator=False)
        action_2 = pd.read_csv(ACTION_201603_FILE, header=0, iterator=False)
        action_3 = pd.read_csv(ACTION_201604_FILE, header=0, iterator=False)
        actions = pd.concat([action_1, action_2, action_3]) # type: pd.DataFrame
        actions = actions[(actions.time >= start_date) & (actions.time < end_date)]
        pickle.dump(actions, open(dump_path, 'wb'))
    return actions
    
# 行为前后关系统计
def get_rate_appeared_yesterday(df_same_prev, df_buy_current):
    """
    给定两个df:
        df_same_prev:user,item,type为
        df_buy_current:某天的购买记录，user,item去重
    返回两个list:
        1.某一天的所有购买的(user,item)对在前一天出现的比例,对应不同的action
        2.某一天的所有购买的user在前一天出现的比例，对应不同的action
    """
    pair_appeared_prevday_rate = []
    total_buy_user_item_num = len(df_buy_current)
    # 对不同的action计算出现的概率
    for action in action_list:
        rate = len(df_same_prev[df_same_prev['type'] == action])/total_buy_user_item_num
        pair_appeared_prevday_rate.append(rate)
    # 计算总的出现的概率    
    unique_prev = len(df_same_prev.drop_duplicates(['user_id','sku_id']))
    pair_appeared_prevday_rate.append(unique_prev/total_buy_user_item_num)
    
    user_appeared_prevday_rate = []
    #对user_id去重
    df_buy_user_current = df_buy_current.drop_duplicates(['user_id'])
    total_buy_user_num = len(df_buy_user_current)

    df_same_prev = df_same_prev.drop_duplicates(['user_id','type'])
    for action in action_list:
        rate = len(df_same_prev[df_same_prev['type'] == action])/total_buy_user_num
        user_appeared_prevday_rate.append(rate)
    
    unique_prev = len(df_same_prev.drop_duplicates(['user_id']))
    user_appeared_prevday_rate.append(unique_prev/total_buy_user_num)
    return pair_appeared_prevday_rate, user_appeared_prevday_rate
    
        
def get_backward_rate_info(df, month,day, with_sepicied_product = False):
    """
    给定某天的数据，计算该天的购买用户商品对在前一天也出现的情况(针对不同action)
    """
    # 读取某天的数据，取出有购买行为的用户商品对
    df_current_day = get_data_in_date(df, month, day)
    df_buy = df_current_day[df_current_day['type'] == BUY_ACTION][['user_id','sku_id']]
    # 取出不重复的购买的用户商品对
    df_buy = df_buy.drop_duplicates(['user_id','sku_id'],keep='last')
    # 取出前一天的用户数据，对user,item,type进行去重
    df_prev_day = get_data_in_date(df, month, day - 1)
    df_prev_day = df_prev_day[['user_id','sku_id','type']]
    result = pd.merge(df_buy, df_prev_day, on = ['user_id','sku_id'], )       
    result = result.drop_duplicates(['user_id','sku_id','type'],keep='last')
    # 对不同的action进行统计
    return get_rate_appeared_yesterday(result, df_buy)
    
def get_forward_rate_info(df, month,day, sku_id):
    """
    给定某天的数据，计算该天不同action的交互行为在后5天内产生购买行为的比例)
    sku_id需要传入set,否则遍历会非常慢
    """
    #对user_item对进行统计
    df_current_day = get_data_in_date(df, month, day)
    df_current_day = df_current_day.drop_duplicates(['user_id','sku_id','type'])[['user_id','sku_id','type']]
    
    df_future_days = get_data_in_date(df, month, day+1, month, day+5)
    df_future_days = df_future_days[df_future_days['type'] == BUY_ACTION]
    df_future_days = df_future_days.drop_duplicates(['user_id','sku_id'])
    df_future_days = df_future_days[['user_id','sku_id']]
    result = pd.merge(df_current_day, df_future_days, on = ['user_id','sku_id'])       
    
    action_pair_precision = []
    action_num = df_current_day.groupby('type').apply(len)
    for action in action_list:
        rate = len(result[result['type'] == action])/action_num.loc[action]
        action_pair_precision.append(rate) 
        
    #对user进行统计
    action_user_precision = []

    #取出对指定sku_id有购买行为的
    criterion = df_future_days['sku_id'].map(lambda x: x in sku_id)
    df_future_days_product =df_future_days[criterion]
    df_future_days_product =  df_future_days_product.drop_duplicates('user_id')
    df_current_day = df_current_day.drop_duplicates(['user_id','type']) 
    result = pd.merge(df_current_day, df_future_days_product, on = ['user_id'])       

    result = result.drop_duplicates(['user_id','type'])
    action_num = df_current_day.groupby('type').apply(len)
    for action in action_list:
        rate = len(result[result['type'] == action])/action_num.loc[action]
        action_user_precision.append(rate) 
    return action_pair_precision, action_user_precision
    
def add_time_to_df(df,start_month, start_day, end_month,end_day):
    return None
    
# 商品相关信息统计
def get_item_statistic(df_item):
    category_dis = df_item.groupby('cate').apply(len)
    brand_dis = df_item.groupby('brand').apply(len)
    return category_dis, brand_dis
        
def getF1(precision, recall):
    return 2*precision*recall/(precision + recall)
    
def getF1_1(precision, recall):
    # 非常偏重precision
    return 6*precision*recall/(5*recall + precision)
def getF1_2(precision, recall):
    return 5*precision*recall/(2*recall + 3*precision)
    
# 测试相关
def get_labels(df, start_date, end_date):
    """
    返回在start_date和end_date之间存在购买的user_item_pair
    """
    dump_path = ACTIOIN_PKL_PATH + 'labels_%s_%s.pkl' % (start_date, end_date)
    if os.path.exists(dump_path):
        actions = pickle.load(open(dump_path))
    else:
        actions = get_data_from_date(df, start_date, end_date)
        actions = actions[actions['type'] == BUY_ACTION]
        actions = actions.groupby(['user_id', 'sku_id'], as_index=False).sum()
        actions['label'] = 1
        actions = actions[['user_id', 'sku_id', 'label']]
        pickle.dump(actions, open(dump_path, 'w'))
    return actions

def report(pred, label):
    """
    根据预测数据和真实数据计算对应的分数
    """
    actions = label
    result = pred

    # 所有用户商品对
    all_user_item_pair = actions['user_id'].map(str) + '-' + actions['sku_id'].map(str)
    all_user_item_pair = np.array(all_user_item_pair)
    # 所有购买用户
    all_user_set = actions['user_id'].unique()

    # 所有品类中预测购买的用户
    all_user_test_set = result['user_id'].unique()
    all_user_test_item_pair = result['user_id'].map(str) + '-' + result['sku_id'].map(str)
    all_user_test_item_pair = np.array(all_user_test_item_pair)

    # 计算所有用户购买评价指标
    pos, neg = 0,0
    for user_id in all_user_test_set:
        if user_id in all_user_set:
            pos += 1
        else:
            neg += 1
    all_user_acc = 1.0 * pos / ( pos + neg)
    all_user_recall = 1.0 * pos / len(all_user_set)
    print ('所有用户中预测购买用户的准确率为 ' + str(all_user_acc))
    print ('所有用户中预测购买用户的召回率' + str(all_user_recall))

    pos, neg = 0, 0
    for user_item_pair in all_user_test_item_pair:
        if user_item_pair in all_user_item_pair:
            pos += 1
        else:
            neg += 1
    all_item_acc = 1.0 * pos / ( pos + neg)
    all_item_recall = 1.0 * pos / len(all_user_item_pair)
    print ('所有用户中预测购买商品的准确率为 ' + str(all_item_acc))
    print ('所有用户中预测购买商品的召回率' + str(all_item_recall))
    F11 = 6.0 * all_user_recall * all_user_acc / (5.0 * all_user_recall + all_user_acc)
    F12 = 5.0 * all_item_acc * all_item_recall / (2.0 * all_item_recall + 3 * all_item_acc)
    score = 0.4 * F11 + 0.6 * F12
    print ('F11=' + str(F11))
    print ('F12=' + str(F12))
    print ('score=' + str(score))    