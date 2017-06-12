# coding=gbk
import pandas as pd
import os
import pickle
import numpy as np

filename = 'user_app_actions.csv'
dirname = 'data/'
user_app_action_dump_path = 'saved_file/user_app_action.pkl'
user_app_rate_path_prefix = 'saved_file/user_app_rate_oneday_'
user_app_rate_path_before_oneday_prefix = 'saved_file/user_app_rate_before_oneday_'
user_app_rate_path_before_oneweek_prefix = 'saved_file/user_app_rate_before_oneweek_'


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


#用户一周前的安装比
def getBeforeOneWeekAppInstallRate(time):

    user_app_rate_before_oneweek_path = user_app_rate_path_before_oneweek_prefix + str(time)
    if os.path.exists(user_app_rate_before_oneweek_path):
        action_total = pickle.load(open(user_app_rate_before_oneweek_path, 'rb'))
        user_app_data = action_total[0]
        user_app_rate_mean = action_total[1]
        app_user_rate_mean = action_total[2]
    else:
        '''
        if os.path.exists(user_app_action_dump_path):
            user_app_data = pickle.load(open(user_app_action_dump_path,'rb'))
        else:
        '''
        user_app_data, _, _, _ = getUserAppActionCatagory()

        user_app_data_before_oneweek = user_app_data[(user_app_data['install_day'] < time) & (user_app_data['install_day'] >= (time-7))]
        if len(user_app_data_before_oneweek) > 0:

            app_amount = len(user_app_data_before_oneweek['appID'])  # 不要用unique，用户可能多次安装
            user_amount = len(user_app_data_before_oneweek['userID'])

            user_app_data_groupby = user_app_data_before_oneweek.groupby('userID')
            app_user_data_groupby = user_app_data_before_oneweek.groupby('appID')

            user_app_rate = user_app_data_groupby.apply(lambda x: len(x['appID']) / app_amount)
            user_app_rate_dataframe = user_app_rate.to_frame(name=('user_app_rate_before_oneweek' + str(time))).reset_index()
            user_app_rate_mean = user_app_rate_dataframe[('user_app_rate_before_oneweek' + str(time))].mean()

            app_user_rate = app_user_data_groupby.apply(lambda x: len(x['userID']) / user_amount)
            app_user_rate_dataframe = app_user_rate.to_frame(name=('app_user_rate_before_oneweek' + str(time))).reset_index()
            app_user_rate_mean = app_user_rate_dataframe[('app_user_rate_before_oneweek' + str(time))].mean()

            user_app_data = pd.merge(user_app_data, user_app_rate_dataframe, how='left', on='userID')
            user_app_data = pd.merge(user_app_data, app_user_rate_dataframe, how='left', on='appID')
            user_app_data['user_app_rate_before_oneweek' + str(time)].fillna(0, inplace=True)
            user_app_data['app_user_rate_before_oneweek' + str(time)].fillna(0, inplace=True)
            pickle.dump([user_app_data, user_app_rate_mean, app_user_rate_mean],
                        open(user_app_rate_before_oneweek_path, 'wb'))
        else:
            user_app_rate_mean = 0
            app_user_rate_mean = 0
    return user_app_data, user_app_rate_mean, app_user_rate_mean

#用户当天前的安装比
def getBeforeOneDayAppInstallRate(time):

    user_app_rate_before_oneday_path = user_app_rate_path_before_oneday_prefix + str(time)
    if os.path.exists(user_app_rate_before_oneday_path):
        action_total = pickle.load(open(user_app_rate_before_oneday_path, 'rb'))
        user_app_data = action_total[0]
        user_app_rate_mean = action_total[1]
        app_user_rate_mean = action_total[2]
    else:
        '''
        if os.path.exists(user_app_action_dump_path):
            user_app_data = pickle.load(open(user_app_action_dump_path,'rb'))
        else:
        '''
        user_app_data, _, _, _ = getUserAppActionCatagory()

        user_app_data_before_oneday = user_app_data[user_app_data['install_day'] < time]
        if len(user_app_data_before_oneday) > 0:

            app_amount = len(user_app_data_before_oneday['appID'])  # 不要用unique，用户可能多次安装
            user_amount = len(user_app_data_before_oneday['userID'])

            user_app_data_groupby = user_app_data_before_oneday.groupby('userID')
            app_user_data_groupby = user_app_data_before_oneday.groupby('appID')

            user_app_rate = user_app_data_groupby.apply(lambda x: len(x['appID']) / app_amount)
            user_app_rate_dataframe = user_app_rate.to_frame(name=('user_app_rate_before' + str(time))).reset_index()
            user_app_rate_mean = user_app_rate_dataframe[('user_app_rate_before' + str(time))].mean()

            app_user_rate = app_user_data_groupby.apply(lambda x: len(x['userID']) / user_amount)
            app_user_rate_dataframe = app_user_rate.to_frame(name=('app_user_rate_before' + str(time))).reset_index()
            app_user_rate_mean = app_user_rate_dataframe[('app_user_rate_before' + str(time))].mean()

            user_app_data = pd.merge(user_app_data, user_app_rate_dataframe, how='left', on='userID')
            user_app_data = pd.merge(user_app_data, app_user_rate_dataframe, how='left', on='appID')
            user_app_data['user_app_rate_before' + str(time)].fillna(0, inplace=True)
            user_app_data['app_user_rate_before' + str(time)].fillna(0, inplace=True)
            pickle.dump([user_app_data, user_app_rate_mean, app_user_rate_mean], open(user_app_rate_before_oneday_path, 'wb'))
        else:
            user_app_rate_mean = 0
            app_user_rate_mean = 0
    return user_app_data, user_app_rate_mean, app_user_rate_mean


#用户１－３１号每天的安装比
def getOneDayAppInstallRate(time):
    '''

    :param time: int
    :return:
    '''
    user_app_rate_oneday_path = user_app_rate_path_prefix + str(time)
    if os.path.exists(user_app_rate_oneday_path):
        action_total = pickle.load(open(user_app_rate_oneday_path,'rb'))
        user_app_data = action_total[0]
        user_app_rate_mean = action_total[1]
        app_user_rate_mean = action_total[2]
    else:
        '''
        if os.path.exists(user_app_action_dump_path):
            user_app_data = pickle.load(open(user_app_action_dump_path,'rb'))
        else:
        '''
        user_app_data,_,_,_ = getUserAppActionCatagory()
        user_app_data_single_day = user_app_data[user_app_data['install_day'] == time]

        if len(user_app_data_single_day) > 0:

            app_amount = len(user_app_data_single_day['appID'])    #不要用unique，用户可能多次安装
            user_amount = len(user_app_data_single_day['userID'])

            user_app_data_groupby = user_app_data_single_day.groupby('userID')
            app_user_data_groupby = user_app_data_single_day.groupby('appID')

            user_app_rate = user_app_data_groupby.apply(lambda x:len(x['appID'])/app_amount)
            user_app_rate_dataframe = user_app_rate.to_frame(name = ('user_app_rate_'+str(time))).reset_index()
            user_app_rate_mean = user_app_rate_dataframe[('user_app_rate_'+str(time))].mean()

            app_user_rate = app_user_data_groupby.apply(lambda x:len(x['userID'])/user_amount)
            app_user_rate_dataframe = app_user_rate.to_frame(name = ('app_user_rate_'+str(time))).reset_index()
            app_user_rate_mean = app_user_rate_dataframe[('app_user_rate_'+str(time))].mean()

            user_app_data = pd.merge(user_app_data,user_app_rate_dataframe,how='left',on='userID')
            user_app_data = pd.merge(user_app_data,app_user_rate_dataframe,how='left',on='appID')
            user_app_data['user_app_rate_'+str(time)].fillna(0,inplace = True)
            user_app_data['app_user_rate_'+str(time)].fillna(0,inplace = True)
            pickle.dump([user_app_data,user_app_rate_mean,app_user_rate_mean],open(user_app_rate_oneday_path,'wb'))
        else:
            user_app_rate_mean =  0
            app_user_rate_mean = 0
    return user_app_data,user_app_rate_mean,app_user_rate_mean


#1-30天数据
def getUserAppActionCatagory():
    user_app_data = pd.read_csv(dirname + filename)

    if os.path.exists(user_app_action_dump_path):
        user_app_total = pickle.load(open(user_app_action_dump_path, 'rb'))
        user_app = user_app_total[0]
        day_frequency = user_app_total[1]
        hour_frequency = user_app_total[2]
        minute_frequency = user_app_total[3]
    else:
        installTime = user_app_data['installTime']
        installTimeSeries = installTime.apply(lambda x :time_transfer(x))
        installTimeDataFrame = pd.DataFrame.from_records(installTimeSeries.tolist(),
                                                         columns=['install_day','install_hour','install_minute','install_second'])

        # day_value_count = installTimeDataFrame['install_day'].value_counts()
        # day_frequency = day_value_count.index.values[1]
        #
        #
        # hour_value_count = installTimeDataFrame['install_hour'].value_counts()
        # hour_frequency = hour_value_count.index.values[1]
        #
        # minute_value_count = installTimeDataFrame['install_minute'].value_counts()
        # minute_frequency = minute_value_count.index.values[1]

        user_app_data = user_app_data.drop('installTime',axis=1)
        user_app = pd.concat([user_app_data,installTimeDataFrame],axis=1)
        pickle.dump(user_app, open(user_app_action_dump_path, 'wb'))

    # return user_app,day_frequency,hour_frequency,minute_frequency


if __name__ == '__main__':
    filename = 'user_app_actions.csv'
    #user_app_data = getdata(dirname + filename)
    getUserAppActionCatagory()
    #getOneDayAppInstallRate(18)
    #getBeforeOneDayAppInstallRate(18)
    # getBeforeOneWeekAppInstallRate(18)
