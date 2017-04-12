# -*- coding: utf-8 -*-
"""
Created on Tue Apr 11 10:21:54 2017

@author: Administrator
"""
from jdata_util import *
from datetime import datetime
from datetime import timedelta
import pickle
import os

def get_comment_date():
    """
    返回comment对应的日期，结果为一个list
    """
    comment_date = []
    start_date = datetime.strptime("2016-02-01", '%Y-%m-%d')
    end_date = datetime.strptime("2016-04-15", "%Y-%m-%d")
    time = start_date
    while time < end_date:
        comment_date.append(datetime.strftime(time + timedelta(days=7), "%Y-%m-%d"))
        time += timedelta(days=7)
    comment_date.append(datetime.strftime(end_date, "%Y-%m-%d"))
    return comment_date

def smooth_rate(x, y, prior = 0, n = 0):
    """
    返回x/y的平滑估计，防止数据量过小的奇异情况
    x:观测正样本
    y:总观测数量
    prior:先验概率
    n:等效样本数量
    """
    return (x+prior*n)/(y+n)
def get_user_feature(df):

    n=5
#    df = pd.get_dummies(df_time_selected['type'], prefix='U_action')
#    df_actions = pd.concat([df_time_selected['user_id'], df] ,axis=1)
#    df_actions = df_actions.groupby('user_id', as_index = False).sum()

    df_count = df.groupby(['user_id','type'], as_index=False).size().reset_index()
    df_actions = pd.pivot_table(df_count, values = 0, index= ['user_id'],columns = ['type'],fill_value = 0)
    prefix = 'U_action_'
    new_name_list = {key:prefix+str(key) for key in df_actions.keys()}
    df_actions.rename(columns=new_name_list, inplace = True)
    df_actions.reset_index(level=0, inplace=True)
    mean_count= df_actions.mean()
    
    for index, action in enumerate(action_list):
        if action!=BUY_ACTION:
              prior = mean_count['U_action_'+str(BUY_ACTION)]/mean_count['U_action_'+str(action)]
              name = 'U_%s/%s_rate'%(action_str[BUY_ACTION-1], action_str[index])
              df_actions[name] = smooth_rate(df_actions['U_action_'+str(BUY_ACTION)], 
                                        df_actions['U_action_'+str(action)],
                                        prior, n)
    return df_actions    
    
def get_comment_feature(start_date, end_date, comment_table, path = ITEM_FEATURE_PATH):
    """
    input:
        start_date:起始时间
        end_date:结束时间
        comment_table:评论表
    output:
        df:s
    """
    dump_path = path + 'comments_accumulate_%s_%s.pkl' % (start_date, end_date)
    if os.path.exists(dump_path):
        comments = pickle.load(open(dump_path, 'rb'))
    else:
        comment_date_end = end_date
        comment_date =  get_comment_date()
        comment_date_begin = comment_date[0]

        for date in reversed(comment_date):
            # 取出最接近end_date的comment_date
            if date < comment_date_end:
                comment_date_begin = date
                break
        comments = comment_table[(comment_table.dt >= comment_date_begin) & 
                                 (comment_table.dt < comment_date_end)]
        comments.drop(['dt'],axis = 1,inplace = True)
        pickle.dump(comments, open(dump_path, 'wb'))
    return comments

def get_item_feature(df_actions, product_table, path = ITEM_FEATURE_PATH):
    """
    input:
        df:action对应的表，已经取过时间间隔
    output:row为sku_id构成的df,每一列为一个feature,其中包含:
        1.action数量,action总量
        2.购买/其余各个操作的比例
        3.同类商品购买排名
        4.
        5.
    """
    dump_path = path+('item_feat_accumulate_%s_%s.pkl' % (start_date, end_date))
    if os.path.exists(dump_path):
        actions = pickle.load(open(dump_path))
    else:
        ## 对商品的行为数进行统计
#        df_feature = pd.get_dummies(df_actions['type'], prefix='I_action')
#        df_actions = pd.concat([df_actions['sku_id'], df_feature], axis=1)
#        actions = df_actions.groupby(['sku_id'], as_index=False).sum()
#        actions = pd.get_dummies(df_count['type'], prefix='Brand_action')        

        df_count = df.groupby(['sku_id','type'], as_index=False).size().reset_index()
        actions = pd.pivot_table(df_count, values = 0, index= ['sku_id'],columns = ['type'],fill_value = 0)
        prefix = 'I_action_'
        new_name_list = {key:prefix+str(key) for key in actions.keys()}
        actions.rename(columns=new_name_list, inplace = True)
        actions.reset_index(level=0, inplace=True)
        #统计先验概率，进行m估计
        mean_count = actions.mean()
        for index,action in enumerate(action_list):
            if action != BUY_ACTION:
                prior = mean_count['I_action_'+str(BUY_ACTION)]/mean_count['I_action_'+str(action)]
                n = 5
                ##n = mean_count['I_action_'+str(action)]
                name = 'I_%s/%s'%(action_str[BUY_ACTION-1], action_str[index])
                actions[name] = smooth_rate(actions['I_action_'+str(BUY_ACTION)], 
                                            actions['I_action_'+str(action)],
                                            prior, n)
        #actions = actions.merge(product_table, how = 'left', on = ['sku_id'])
    return actions

def get_brand_feature(df):
    """
    input:
        df:action对应的表
        product_table:需要预测的商品信息表
    """

    ## 对商品的行为数进行统计
    #目前这样写会有内存泄漏，要用gc.collect
    ##dump_path = path+('brand_feat_accumulate_%s_%s.pkl' % (start_date, end_date))
    if os.path.exists(dump_path):
        actions = pickle.load(open(dump_path))
    else:
        df_count = df.groupby(['brand','type'], as_index=False).size().reset_index()
        actions = pd.pivot_table(df_count, values = 0, index= ['brand'],columns = ['type'],fill_value = 0)
        prefix = 'Brand_action_'
        new_name_list = {key:prefix+str(key) for key in actions.keys()}
        actions.rename(columns=new_name_list, inplace = True)
        actions.reset_index(level=0, inplace=True)
    
        #统计先验概率，进行m估计
        mean_count = actions.mean()
        for index,action in enumerate(action_list):
            if action != BUY_ACTION:
                prior = mean_count['Brand_action_'+str(BUY_ACTION)]/mean_count['Brand_action_'+str(action)]
                ##n = mean_count['Brand_action_'+str(action)]
                n = 5
                name = 'Brand_%s/%s'%(action_str[BUY_ACTION-1], action_str[index])
                actions[name] = smooth_rate(actions['Brand_action_'+str(BUY_ACTION)], 
                                            actions['Brand_action_'+str(action)],
                                            prior, n)
    return actions
            

def make_train_set(df, train_start_date, train_end_date, label_start_date, label_end_date, days=30):
    """
    还没写好
    """
    dump_path = PKL_PATH + 'train_set_%s_%s_%s_%s.pkl' % (train_start_date, train_end_date, test_start_date, test_end_date)
    if os.path.exists(dump_path):
        actions = pickle.load(open(dump_path))
    else:
        start_days = "2016-02-01"
        start_date = "2016-03-01"
        end_date = "2016-03-31"
        # 先找到对应的label,用于后续的merge
        labels = get_labels(df, start_date, end_date)
        # 生成不同的特征
        comment_feature = get_comment_feature(train_start, train_end, comment_table)
        item_feature = get_item_feature(action_table, product_table)
        brand_feature=  get_brand_feature(action_table)
        # generate 时间窗口
        # actions = get_accumulate_action_feat(train_start_date, train_end_date)
        actions = None
#        for i in (1, 2, 3, 5, 7, 10, 15, 21, 30):
#            start_days = datetime.strptime(train_end_date, '%Y-%m-%d') - timedelta(days=i)
#            start_days = start_days.strftime('%Y-%m-%d')
#            if actions is None:
#                actions = get_action_feat(start_days, train_end_date)
#            else:
#                actions = pd.merge(actions, get_action_feat(start_days, train_end_date), how='left',
#                                   on=['user_id', 'sku_id'])

        actions = pd.merge(actions, user, how='left', on='user_id')
        actions = pd.merge(actions, user_acc, how='left', on='user_id')
        actions = pd.merge(actions, product, how='left', on='sku_id')
        actions = pd.merge(actions, product_acc, how='left', on='sku_id')
        actions = pd.merge(actions, comment_acc, how='left', on='sku_id')
        actions = pd.merge(actions, labels, how='left', on=['user_id', 'sku_id'])
        actions = actions.fillna(0)

    users = actions[['user_id', 'sku_id']].copy()
    labels = actions['label'].copy()
    del actions['user_id']
    del actions['sku_id']
    del actions['label']

    return users, actions, labels

    