# coding=gbk
import pandas as pd
import pickle
import os
import numpy as np

filename = 'user_installedapps.csv'
dirname = 'data/'
user_app_dump_path = 'saved_file/user_app_amount.pkl'
app_user_dump_path = 'saved_file/app_user_amount.pkl'
UserInstalledAppCater_info_dump_path = 'saved_file/userInstalledAppCater_info.pkl'
UserInstalledAppCater2_info_dump_path = 'saved_file/userInstalledAppCater2_info.pkl'

#用户安装比例
def getUserInstallAppAction():
    user_install_app = pd.read_csv(dirname + filename)
    user_app_amount_dataframe = None
    app_user_amount_dataframe = None
    if os.path.exists(user_app_dump_path) and os.path.exists(app_user_dump_path):
        if os.path.exists(user_app_dump_path):
            user_app_amount_dataframe_total = pickle.load(open(user_app_dump_path,'rb'))
            user_app_amount_dataframe = user_app_amount_dataframe_total[0]
            user_app_amount_mean = user_app_amount_dataframe_total[1]
            user_app_rate_mean = user_app_amount_dataframe_total[2]

        if os.path.exists(app_user_dump_path):
            app_user_amount_dataframe_total = pickle.load(open(app_user_dump_path,'rb'))
            app_user_amount_dataframe = app_user_amount_dataframe_total[0]
            app_user_amount_mean = app_user_amount_dataframe_total[1]
            app_user_rate_mean = app_user_amount_dataframe_total[2]

    else:

        app_amount = len(user_install_app['appID'].unique())
        user_amount = len(user_install_app['userID'].unique())



        app_groupby_userID = user_install_app.groupby('userID')
        app_groupby_appID = user_install_app.groupby('appID')



        user_app_rate = app_groupby_userID.apply(lambda x:len(x['appID'].unique())/app_amount)
        user_app_rate_dataframe = user_app_rate.to_frame(name='user_app_rate').reset_index()
        user_app_rate_mean = user_app_rate_dataframe['user_app_rate'].mean()


        app_user_rate = app_groupby_appID.apply(lambda x:len(x['userID'].unique())/user_amount)
        app_user_rate_dateframe = app_user_rate.to_frame(name='app_user_rate').reset_index()
        app_user_rate_mean = app_user_rate_dateframe['app_user_rate'].mean()


        user_app_amount_dataframe = app_groupby_userID.count()
        user_app_amount_dataframe = user_app_amount_dataframe.reset_index()
        user_app_amount_dataframe.columns = ['userID','appAmount']
        user_app_amount_dataframe = pd.merge(user_app_amount_dataframe,user_app_rate_dataframe,how='left',on='userID')
        user_app_amount_mean = user_app_amount_dataframe['appAmount'].mean()


        app_user_amount_dataframe = app_groupby_appID.count()
        app_user_amount_dataframe = app_user_amount_dataframe.reset_index()
        app_user_amount_dataframe.columns = ['appID','userAmount']
        app_user_amount_dataframe = pd.merge(app_user_amount_dataframe,app_user_rate_dateframe,how='left',on='appID')
        app_user_amount_mean = app_user_amount_dataframe['userAmount'].mean()

        pickle.dump(user_app_amount_dataframe,open(user_app_dump_path,'wb'))
        pickle.dump(app_user_amount_dataframe,open(app_user_dump_path,'wb'))
        # pickle.dump([user_app_amount_dataframe,user_app_amount_mean,user_app_rate_mean],open(user_app_dump_path,'wb'))
        # pickle.dump([app_user_amount_dataframe,app_user_amount_mean,app_user_rate_mean],open(app_user_dump_path,'wb'))

    # return user_app_amount_dataframe,app_user_amount_dataframe,user_app_amount_mean,app_user_amount_mean,user_app_rate_mean,app_user_rate_mean

#1号之前用户已安装的所有app的属性信息，共28类属性，30维
def getUserInstalledAppCater():
    with np.load("installedApp_caterInfo.npz") as data:
        installedApp_caterInfo = data['X']
        userID_set = data['index']
    installedApp_caterInfo = installedApp_caterInfo.tolist()
    for i in range(len(installedApp_caterInfo)):
        installedApp_caterInfo[i].append(userID_set[i])
    UserInstalledAppCater_df = pd.DataFrame.from_records(installedApp_caterInfo,
    columns=['cater_1','cater_2','cater_3','cater_4','cater_5','cater_6','cater_7'
             ,'cater_8','cater_9','cater_10','cater_11','cater_12','cater_13','cater_14'
             ,'cater_15','cater_16','cater_17','cater_18','cater_19','cater_20','cater_21'
             ,'cater_22','cater_23','cater_24','cater_25','cater_26','cater_27','cater_28'
             ,'cater_sum','cluster_label','userID'])
    pickle.dump(UserInstalledAppCater_df,open(UserInstalledAppCater_info_dump_path,'wb'))

#17号之前用户已安装的所有app的属性信息，共28类属性，30维
def getUserInstalledAppCater_before_17th():
    with np.load("installedApp_caterInfo.npz") as data:
        installedApp_caterInfo = data['X']
        userID_set = data['index']
    installedApp_caterInfo = installedApp_caterInfo.tolist()
    for i in range(len(installedApp_caterInfo)):
        installedApp_caterInfo[i].append(userID_set[i])
    UserInstalledAppCater_df = pd.DataFrame.from_records(installedApp_caterInfo,
    columns=['cater2_1','cater2_2','cater2_3','cater2_4','cater2_5','cater2_6','cater2_7'
             ,'cater2_8','cater2_9','cater2_10','cater2_11','cater2_12','cater2_13','cater2_14'
             ,'cater2_15','cater2_16','cater2_17','cater2_18','cater2_19','cater2_20','cater2_21'
             ,'cater2_22','cater2_23','cater2_24','cater2_25','cater2_26','cater2_27','cater2_28'
             ,'cater2_sum','cluster2_label','userID'])
    pickle.dump(UserInstalledAppCater_df,open(UserInstalledAppCater2_info_dump_path,'wb'))


def installed_same_flag():

    #用户是否安装了当前的appID
    with np.load("installed_same_appID.npz") as data:
        install_flag = data['X']
    install_flag = install_flag.tolist()
    install_flag_df = pd.DataFrame()
    install_flag_df['install_flag'] = install_flag

    #特征 23 维  用户当前时刻之前转化的所有信息，点击小时的max/min/mean/median/std，连接类型5种、
    # app平台3种、服务商4种、siteSet3种、positionType3种
    with np.load("conserved_info.npz") as data:
        conserved_info = data['X']
    conserved_info_df = pd.DataFrame.from_records(conserved_info,
    columns=['click_hour_max','click_hour_min','click_hour_mean'
             ,'click_hour_median','click_hour_std','connectionType_0','connectionType_1'
             ,'connectionType_2','connectionType_3','connectionType_4','appPlatform_0'
             ,'appPlatform_1','appPlatform_2','telecomsOperator_0','telecomsOperator_1'
             ,'telecomsOperator_2','telecomsOperator_3','siteSet_0','siteSet_1'
             ,'siteSet_2','positionType_0','positionType_1','positionType_2'])
    conserved_info_df = pd.concat([conserved_info_df, install_flag_df], axis=1)
    pickle.dump(conserved_info_df,open(conserved_info_dump_path,'wb'))

def ad_history_info():
    #特征 23 维  用户当前时刻之前转化的所有信息，点击小时的max/min/mean/median/std，连接类型5种、
    # app平台3种、服务商4种、siteSet3种、positionType3种
    #针对creativeID的角度
    with np.load("ad_conserved_info.npz") as data:
        ad_conserved_info = data['X']
    ad_conserved_info_df = pd.DataFrame.from_records(ad_conserved_info,
    columns=['ad_click_hour_max','ad_click_hour_min','ad_click_hour_mean'
             ,'ad_click_hour_median','ad_click_hour_std','ad_connectionType_0','ad_connectionType_1'
             ,'ad_connectionType_2','ad_connectionType_3','ad_connectionType_4','ad_appPlatform_0'
             ,'ad_appPlatform_1','ad_appPlatform_2','ad_telecomsOperator_0','ad_telecomsOperator_1'
             ,'ad_telecomsOperator_2','ad_telecomsOperator_3','ad_siteSet_0','ad_siteSet_1'
             ,'ad_siteSet_2','ad_positionType_0','ad_positionType_1','ad_positionType_2'])
    #针对appID的角度
    with np.load("app_conserved_info.npz") as data:
        app_conserved_info = data['X']
    app_conserved_info_df = pd.DataFrame.from_records(app_conserved_info,
    columns=['app_click_hour_max','app_click_hour_min','app_click_hour_mean'
             ,'app_click_hour_median','app_click_hour_std','app_connectionType_0','app_connectionType_1'
             ,'app_connectionType_2','app_connectionType_3','app_connectionType_4','app_appPlatform_0'
             ,'app_appPlatform_1','app_appPlatform_2','app_telecomsOperator_0','app_telecomsOperator_1'
             ,'app_telecomsOperator_2','app_telecomsOperator_3','app_siteSet_0','app_siteSet_1'
             ,'app_siteSet_2','app_positionType_0','app_positionType_1','app_positionType_2'])

    ad_conserved_info_df = pd.concat([ad_conserved_info_df, app_conserved_info_df], axis=1)
    pickle.dump(ad_conserved_info_df,open(ad_conserved_info_dump_path,'wb'))

if __name__ == '__main__':
    filename = 'user_installedapps.csv'
    #user_install_app = getData(dirname+filename)
    getUserInstallAppAction()
    # getUserInstalledAppCater()
    # getUserInstalledAppCater_before_17th()
    # userClusterFeature()
    # adClusterFeature()
    # cpr_relate()
    # installed_same_flag()
    # ad_history_info()