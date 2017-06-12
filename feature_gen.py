# coding=gbk
import pandas as pd
import pickle
import os
import numpy as np

filename = 'train.csv'
dirname = 'data/'
merge_src_train_dump_path = 'saved_file/merge_src_train_data.pkl'
merge_src_test_dump_path = 'saved_file/merge_src_test_data.pkl'
train_data_dump_path = 'saved_file/train.pkl'
test_data_dump_path = 'saved_file/test.pkl'

#时间转换为 day,hour,minute
def time_transfer(value):
    '''

    :param value:int
    :return: dataframe
    '''

    day = 0
    hour = 0
    minute = 0
    second = 0

    timeStr = str(value)
    if len(timeStr) == 8:
        day = int(timeStr[:2])
        hour = int(timeStr[2:4])
        minute = int(timeStr[4:6])
        second = int(timeStr[6:])

    return day,hour,minute,second

#重复点击次数及与上次点击时间差（分钟）
def repeat_num(df,x):
    return len(df[(df.userID==x['userID'])&(df.creativeID==x['creativeID'])&(df.clickTime<x['clickTime'])])
'''
def repeat_click(df):
    repeat_click_count = df.apply(lambda row: repeat_num(df,row), axis=1)
    return repeat_click_count
'''
def time_subtract(time1,time2):
    timeStr1 = str(time1)
    timeStr2 = str(time2)
    day1 = int(timeStr1[:2])
    hour1 = int(timeStr1[2:4])
    minute1 = int(timeStr1[4:6])
    second1 = int(timeStr1[6:])
    day2 = int(timeStr2[:2])
    hour2 = int(timeStr2[2:4])
    minute2 = int(timeStr2[4:6])
    second2 = int(timeStr2[6:])
    return (day2-day1)*24*60+(hour2-hour1)*60+(minute2-minute1)+(second2-second1)/60
def time_interval(df,x):
    time_seris = df[(df.userID==x['userID'])&(df.creativeID==x['creativeID'])&
            (df.clickTime<x['clickTime'])]['clickTime'].astype(int)
    if len(time_seris)>0:
        time1 = time_seris.max()
        return time_subtract(time1,x['clickTime'])
    else:
        return 0

# 用户点击数、点击率（此刻之前、前一周、前一天、当天中）
# 用户转化数、转化率（当天之前、前一天、前一周）
def get_user_cpr(df,row):
    click_day = row['click_day'];click_time = row['clickTime'];user = row['userID']
    before_now_df = df[(df.userID==user)&(df.clickTime<click_time)]
    click_before_today_df = df[(df.userID==user)&(df.click_day<click_day)]
    total_click_before_today = len(click_before_today_df)
    conversion_before_now_df = click_before_today_df[click_before_today_df.label==1]

    before_now_all_df = df[df.clickTime<click_time]
    click_num_before_now = len(before_now_df);click_all_num_before_now = len(before_now_all_df)
    cpr_before_now = click_num_before_now/(click_all_num_before_now+1e-5)

    week_ago_df = df[(df.userID==user)&(df.click_day<click_day)&(df.click_day>=click_day-7)]
    conversion_week_ago_df = week_ago_df[week_ago_df.label==1]
    week_ago_all_df = df[(df.click_day<click_day)&(df.click_day>=click_day-7)]
    click_num_week_ago = len(week_ago_df);click_num_all_week_ago = len(week_ago_all_df)
    cpr_week_ago = click_num_week_ago/(click_num_all_week_ago+1e-5)

    day_ago_df = df[(df.userID==user)&(df.click_day<click_day)&(df.click_day>=click_day-1)]
    conversion_day_ago_df = day_ago_df[day_ago_df.label==1]
    day_ago_all_df = df[(df.click_day<click_day)&(df.click_day>=click_day-1)]
    click_num_day_ago = len(day_ago_df);click_num_all_day_ago = len(day_ago_all_df)
    cpr_day_ago = click_num_day_ago/(click_num_all_day_ago+1e-5)

    today_df = df[(df.userID==user)&(df.click_day==click_day)]
    #今天内的第几次点击
    today_at_df = df[(df.userID==user)&(df.click_day==click_day)&(df.clickTime<=click_time)]
    today_all_df = df[df.click_day==click_day]
    click_num_today = len(today_df);click_num_all_today = len(today_all_df)
    cpr_today = click_num_today/(click_num_all_today+1e-5)
    click_times_today = len(today_at_df)

    #CVR
    conversion_before_today_num = len(conversion_before_now_df)
    cvr_before_today = len(conversion_before_now_df)/(total_click_before_today+1e-5)
    conversion_week_ago_num = len(conversion_week_ago_df)
    cvr_week_ago = len(conversion_week_ago_df)/(click_num_week_ago+1e-5)
    conversion_day_ago_num = len(conversion_day_ago_df)
    cvr_day_ago = len(conversion_day_ago_df)/(click_num_day_ago+1e-5)


    return click_num_before_now,cpr_before_now,click_num_week_ago,cpr_week_ago,click_num_day_ago\
        ,cpr_day_ago,click_num_today,cpr_today,click_times_today,cvr_before_today,cvr_week_ago\
        ,cvr_day_ago,conversion_before_today_num,conversion_week_ago_num,conversion_day_ago_num

# appID点击数、点击率（此刻之前、前一周、前一天、当天中）
# appID转化数、转化率（当天之前、前一天、前一周）
def get_app_cpr(df,row):
    click_day = row['click_day'];click_time = row['clickTime'];appID = row['appID']
    before_now_df = df[(df.appID==appID)&(df.clickTime<click_time)]
    click_before_today_df = df[(df.appID==appID)&(df.click_day<click_day)]
    total_click_before_today = len(click_before_today_df)
    conversion_before_now_df = click_before_today_df[click_before_today_df.label==1]

    before_now_all_df = df[df.clickTime<click_time]
    click_num_before_now = len(before_now_df);click_all_num_before_now = len(before_now_all_df)
    cpr_before_now = click_num_before_now/(click_all_num_before_now+1e-5)

    week_ago_df = df[(df.appID==appID)&(df.click_day<click_day)&(df.click_day>=click_day-7)]
    conversion_week_ago_df = week_ago_df[week_ago_df.label==1]
    week_ago_all_df = df[(df.click_day<click_day)&(df.click_day>=click_day-7)]
    click_num_week_ago = len(week_ago_df);click_num_all_week_ago = len(week_ago_all_df)
    cpr_week_ago = click_num_week_ago/(click_num_all_week_ago+1e-5)

    day_ago_df = df[(df.appID==appID)&(df.click_day<click_day)&(df.click_day>=click_day-1)]
    conversion_day_ago_df = day_ago_df[day_ago_df.label==1]
    day_ago_all_df = df[(df.click_day<click_day)&(df.click_day>=click_day-1)]
    click_num_day_ago = len(day_ago_df);click_num_all_day_ago = len(day_ago_all_df)
    cpr_day_ago = click_num_day_ago/(click_num_all_day_ago+1e-5)

    today_df = df[(df.appID==appID)&(df.click_day==click_day)]
    today_all_df = df[df.click_day==click_day]
    click_num_today = len(today_df);click_num_all_today = len(today_all_df)
    cpr_today = click_num_today/(click_num_all_today+1e-5)

    #CVR
    conversion_before_today_num = len(conversion_before_now_df)
    cvr_before_today = len(conversion_before_now_df)/(total_click_before_today+1e-5)
    conversion_week_ago_num = len(conversion_week_ago_df)
    cvr_week_ago = len(conversion_week_ago_df)/(click_num_week_ago+1e-5)
    conversion_day_ago_num = len(conversion_day_ago_df)
    cvr_day_ago = len(conversion_day_ago_df)/(click_num_day_ago+1e-5)


    return click_num_before_now,cpr_before_now,click_num_week_ago,cpr_week_ago,click_num_day_ago\
        ,cpr_day_ago,click_num_today,cpr_today,cvr_before_today,cvr_week_ago\
        ,cvr_day_ago,conversion_before_today_num,conversion_week_ago_num,conversion_day_ago_num

# creativeID点击数、点击率（此刻之前、前一周、前一天、当天中）
# creativeID转化数、转化率（当天之前、前一天、前一周）
def get_creative_cpr(df,row):
    click_day = row['click_day'];click_time = row['clickTime'];creativeID = row['creativeID']
    before_now_df = df[(df.creativeID==creativeID)&(df.clickTime<click_time)]
    click_before_today_df = df[(df.creativeID==creativeID)&(df.click_day<click_day)]
    total_click_before_today = len(click_before_today_df)
    conversion_before_now_df = click_before_today_df[click_before_today_df.label==1]

    before_now_all_df = df[df.clickTime<click_time]
    click_num_before_now = len(before_now_df);click_all_num_before_now = len(before_now_all_df)
    cpr_before_now = click_num_before_now/(click_all_num_before_now+1e-5)

    week_ago_df = df[(df.creativeID==creativeID)&(df.click_day<click_day)&(df.click_day>=click_day-7)]
    conversion_week_ago_df = week_ago_df[week_ago_df.label==1]
    week_ago_all_df = df[(df.click_day<click_day)&(df.click_day>=click_day-7)]
    click_num_week_ago = len(week_ago_df);click_num_all_week_ago = len(week_ago_all_df)
    cpr_week_ago = click_num_week_ago/(click_num_all_week_ago+1e-5)

    day_ago_df = df[(df.creativeID==creativeID)&(df.click_day<click_day)&(df.click_day>=click_day-1)]
    conversion_day_ago_df = day_ago_df[day_ago_df.label==1]
    day_ago_all_df = df[(df.click_day<click_day)&(df.click_day>=click_day-1)]
    click_num_day_ago = len(day_ago_df);click_num_all_day_ago = len(day_ago_all_df)
    cpr_day_ago = click_num_day_ago/(click_num_all_day_ago+1e-5)

    today_df = df[(df.creativeID==creativeID)&(df.click_day==click_day)]
    today_all_df = df[df.click_day==click_day]
    click_num_today = len(today_df);click_num_all_today = len(today_all_df)
    cpr_today = click_num_today/(click_num_all_today+1e-5)

    #CVR
    conversion_before_today_num = len(conversion_before_now_df)
    cvr_before_today = len(conversion_before_now_df)/(total_click_before_today+1e-5)
    conversion_week_ago_num = len(conversion_week_ago_df)
    cvr_week_ago = len(conversion_week_ago_df)/(click_num_week_ago+1e-5)
    conversion_day_ago_num = len(conversion_day_ago_df)
    cvr_day_ago = len(conversion_day_ago_df)/(click_num_day_ago+1e-5)


    return click_num_before_now,cpr_before_now,click_num_week_ago,cpr_week_ago,click_num_day_ago\
        ,cpr_day_ago,click_num_today,cpr_today,cvr_before_today,cvr_week_ago\
        ,cvr_day_ago,conversion_before_today_num,conversion_week_ago_num,conversion_day_ago_num

#为了速度，仅提取后一周的数据特征来训练
def train_feature_gen(df):

    df = df.fillna(0)

    #转换转化时间
    conversionTime = df['conversionTime']
    conversionTimeSeries = conversionTime.apply(lambda x:time_transfer(x))
    conversionTimeDataFrame = pd.DataFrame.from_records(conversionTimeSeries.tolist(),
                                                        columns=['conversion_day','conversion_hour',
                                                                 'conversion_minute','conversion_second'])
    df = pd.concat([df,conversionTimeDataFrame], axis=1)

    res_df = df[df.click_day>=24]

    #重复点击计数
    repeat_click_count = res_df.apply(lambda row: repeat_num(df,row), axis=1)
    res_df['repeat_click_count'] = repeat_click_count
    res_df["repeat_time_interval"] = list(res_df.apply(lambda row: time_interval(df,row), axis=1))
    # 用户点击数、点击率（当天前、前一周、前一天、当天中）
    user_ctr_relate = res_df.apply(lambda row: get_user_cpr(df,row), axis=1)
    # app点击数、点击率（当天前、前一周、前一天、当天中）
    app_ctr_relate = res_df.apply(lambda row: get_app_cpr(df,row), axis=1)
    # creative点击数、点击率（当天前、前一周、前一天、当天中）
    creative_ctr_relate = res_df.apply(lambda row: get_creative_cpr(df,row), axis=1)
    user_ctr_relate_df = pd.DataFrame.from_records(user_ctr_relate.tolist(),
                                                   columns=['user_click_num_before_now','user_ctr_before_now'
                                                       ,'user_click_num_week_ago','user_ctr_week_ago'
                                                       ,'user_click_num_day_ago','user_ctr_day_ago'
                                                       ,'user_click_num_today','user_ctr_today','user_click_times_today'
                                                       ,'user_cvr_before_today','user_cvr_week_ago','user_cvr_day_ago'
                                                       ,'user_conversion_before_today_num','user_conversion_week_ago_num'
                                                       ,'user_conversion_day_ago_num'])
    app_ctr_relate_df = pd.DataFrame.from_records(app_ctr_relate.tolist(),
                                                   columns=['app_click_num_before_now','app_ctr_before_now'
                                                       ,'app_click_num_week_ago','app_ctr_week_ago'
                                                       ,'app_click_num_day_ago','app_ctr_day_ago'
                                                       ,'app_click_num_today','app_ctr_today'
                                                       ,'app_cvr_before_today','app_cvr_week_ago','app_cvr_day_ago'
                                                       ,'app_conversion_before_today_num','app_conversion_week_ago_num'
                                                       ,'app_conversion_day_ago_num'])
    creative_ctr_relate_df = pd.DataFrame.from_records(creative_ctr_relate.tolist(),
                                                   columns=['creative_click_num_before_now','creative_ctr_before_now'
                                                       ,'creative_click_num_week_ago','creative_ctr_week_ago'
                                                       ,'creative_click_num_day_ago','creative_ctr_day_ago'
                                                       ,'creative_click_num_today','creative_ctr_today'
                                                       ,'creative_cvr_before_today','creative_cvr_week_ago','creative_cvr_day_ago'
                                                       ,'creative_conversion_before_today_num','creative_conversion_week_ago_num'
                                                       ,'creative_conversion_day_ago_num'])
    res_df = pd.concat([res_df,user_ctr_relate_df,app_ctr_relate_df,creative_ctr_relate_df], axis=1)

    return res_df

def test_feature_gen(all_df,test_df):

    test_df = test_df.fillna(0)

    #重复点击计数
    repeat_click_count = test_df.apply(lambda row: repeat_num(all_df,row), axis=1)
    test_df['repeat_click_count'] = repeat_click_count
    test_df["repeat_time_interval"] = list(test_df.apply(lambda row: time_interval(all_df,row), axis=1))
    # 用户点击数、点击率（当天前、前一周、前一天、当天中）
    user_ctr_relate = test_df.apply(lambda row: get_user_cpr(all_df,row), axis=1)
    # app点击数、点击率（当天前、前一周、前一天、当天中）
    app_ctr_relate = test_df.apply(lambda row: get_app_cpr(all_df,row), axis=1)
    # creative点击数、点击率（当天前、前一周、前一天、当天中）
    creative_ctr_relate = test_df.apply(lambda row: get_creative_cpr(all_df,row), axis=1)
    user_ctr_relate_df = pd.DataFrame.from_records(user_ctr_relate.tolist(),
                                                   columns=['user_click_num_before_now','user_ctr_before_now'
                                                       ,'user_click_num_week_ago','user_ctr_week_ago'
                                                       ,'user_click_num_day_ago','user_ctr_day_ago'
                                                       ,'user_click_num_today','user_ctr_today','user_click_times_today'
                                                       ,'user_cvr_before_today','user_cvr_week_ago','user_cvr_day_ago'
                                                       ,'user_conversion_before_today_num','user_conversion_week_ago_num'
                                                       ,'user_conversion_day_ago_num'])
    app_ctr_relate_df = pd.DataFrame.from_records(app_ctr_relate.tolist(),
                                                   columns=['app_click_num_before_now','app_ctr_before_now'
                                                       ,'app_click_num_week_ago','app_ctr_week_ago'
                                                       ,'app_click_num_day_ago','app_ctr_day_ago'
                                                       ,'app_click_num_today','app_ctr_today'
                                                       ,'app_cvr_before_today','app_cvr_week_ago','app_cvr_day_ago'
                                                       ,'app_conversion_before_today_num','app_conversion_week_ago_num'
                                                       ,'app_conversion_day_ago_num'])
    creative_ctr_relate_df = pd.DataFrame.from_records(creative_ctr_relate.tolist(),
                                                   columns=['creative_click_num_before_now','creative_ctr_before_now'
                                                       ,'creative_click_num_week_ago','creative_ctr_week_ago'
                                                       ,'creative_click_num_day_ago','creative_ctr_day_ago'
                                                       ,'creative_click_num_today','creative_ctr_today'
                                                       ,'creative_cvr_before_today','creative_cvr_week_ago','creative_cvr_day_ago'
                                                       ,'creative_conversion_before_today_num','creative_conversion_week_ago_num'
                                                       ,'creative_conversion_day_ago_num'])
    test_df = pd.concat([test_df,user_ctr_relate_df,app_ctr_relate_df,creative_ctr_relate_df], axis=1)

    return test_df

#merge基本特征
def mergeAllData(fileName,merge_src_data_dump_path):
    data_df = pd.read_csv(fileName)
    ad_df = pickle.load(open('saved_file/ad.pkl','rb'))
    app_categories_df = pickle.load(open('saved_file/app_category.pkl','rb'))
    position_df = pickle.load(open('saved_file/position.pkl','rb'))
    user_df = pickle.load(open('saved_file/user.pkl','rb'))

    data_df = pd.merge(data_df,ad_df,how='left',on='creativeID')
    data_df = pd.merge(data_df,app_categories_df,how='left',on='appID')
    data_df = pd.merge(data_df,position_df,how='left',on='positionID')
    data_df = pd.merge(data_df,user_df,how='left',on='userID')

    clickTime = data_df['clickTime']
    clickTimeSeries = clickTime.apply(lambda x: time_transfer(x))
    clickTimeDataFrame = pd.DataFrame.from_records(clickTimeSeries.tolist(),
                                                   columns=['click_day', 'click_hour',
                                                            'click_minute','click_second'])
    data_df = pd.concat([data_df,clickTimeDataFrame], axis=1)

    pickle.dump(data_df,open(merge_src_data_dump_path,'wb'),protocol=4)

#对clickTime分成DDHHMMSS
def clickTime_deal(data_df,merge_src_data_dump_path):
    clickTime = data_df['clickTime']
    clickTimeSeries = clickTime.apply(lambda x: time_transfer(x))
    clickTimeDataFrame = pd.DataFrame.from_records(clickTimeSeries.tolist(),
                                                   columns=['click_day', 'click_hour',
                                                            'click_minute','click_second'])
    data_df = pd.concat([data_df,clickTimeDataFrame], axis=1)

    pickle.dump(data_df,open(merge_src_data_dump_path,'wb'),protocol=4)

if __name__ == '__main__':
    # mergeAllData(train_filename,merge_src_train_dump_path) 已完成
    # mergeAllData(test_filename,merge_src_test_dump_path)  已完成

    train_filename = 'data/train.csv'
    test_filename = 'data/test.csv'

    ##### train_Set
    print("Generate train.pkl")
    train_data_df = pickle.load(open(merge_src_train_dump_path,'rb'))
    train_df = train_feature_gen(train_data_df)
    pickle.dump(train_df,open(train_data_dump_path,'wb'),protocol=4)


    ##### test_Set
    print("Generate test.pkl")
    test_data_df = pickle.load(open(merge_src_test_dump_path,'rb'))
    # print(test_data_df.columns)
    #使train_df与test_df列名相同再拼接
    train_data_df = train_data_df.drop(['clickTime','conversionTime','creativeID',
                             'positionID','connectionType','telecomsOperator'],axis = 1)

    train_test_df = pd.concat([train_data_df,test_data_df],ignore_index=True)
    test_df = test_feature_gen(train_test_df,test_data_df)
    pickle.dump(test_df,open(test_data_dump_path,'wb'),protocol=4)