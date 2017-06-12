# coding=gbk
import pandas as pd
import pickle
import os
import numpy as np

merge_src_train_dump_path = 'saved_file/merge_src_train_data.pkl'
merge_src_test_dump_path = 'saved_file/merge_src_test_data.pkl'
train_data_dump_path = 'saved_file/train_combine_feat.pkl'
test_data_dump_path = 'saved_file/test_combine_feat.pkl'

com_feat_list = ['positionID','advertiserID','appID','positionType','appCategory'
    ,'appCategory','connectionType','adID','creativeID','sitesetID']
#统计positionID,advertiserID,appID,app_days_cnt,appCategory,
# connectionType,adID,creativeID）统计共同出现的次数、转化率等；45*3维
def combine_feat_count(src_df,conv_df,row):
    click_day = row['click_day']
    # positionID = row['positionID']
    # advertiserID = row['advertiserID']
    # appID = row['appID']
    # appCategory = row['appCategory']
    # connectionType = row['connectionType']
    # adID = row['adID']
    # creativeID = row['creativeID']

    combine_result = []

    for i in range(len(com_feat_list)):
        tem_feat1 = com_feat_list[i]
        j = i+1
        while j<len(com_feat_list):
            tem_feat2 = com_feat_list[j]
            tem_all_df = conv_df[conv_df.click_day<click_day]
            conv_tem_df = conv_df[(conv_df[tem_feat1]==row[tem_feat1])&(conv_df[tem_feat2]==row[tem_feat2])
                             &(conv_df.click_day<click_day)]
            conv_rate = len(conv_tem_df)/(len(tem_all_df)+1e-5)
            #转化个数
            combine_result.append(len(conv_tem_df))
            #占所有转化总数的比
            combine_result.append(conv_rate)
            src_tem_df = src_df[(src_df[tem_feat1]==row[tem_feat1])&(src_df[tem_feat2]==row[tem_feat2])
                             &(src_df.click_day<click_day)]
            #转化与非转化的比
            combine_result.append(len(conv_tem_df)/(len(src_tem_df)-len(conv_tem_df)+1e-5))

            j += 1
    return combine_result

def train_feature_gen(df):

    df = df.fillna(0)
    conv_df = df[df.label==1]

    #组合特征统计量
    combine_feat = df.apply(lambda row: combine_feat_count(df,conv_df,row), axis=1)
    combine_feat_df = pd.DataFrame.from_records(combine_feat.tolist()) #未取列名
    df = pd.concat([df,combine_feat_df], axis=1)

    return df

def test_feature_gen(all_df,test_df):

    test_df = test_df.fillna(0)
    conv_df = all_df[all_df.label==1]

    #组合特征统计量
    combine_feat = test_df.apply(lambda row: combine_feat_count(all_df,conv_df,row), axis=1)
    combine_feat_df = pd.DataFrame.from_records(combine_feat.tolist()) #未取列名
    test_df = pd.concat([test_df,combine_feat_df], axis=1)

    return test_df

if __name__ == '__main__':

    ##### train_Set
    train_data_df = pickle.load(open(merge_src_train_dump_path,'rb'))
    train_df = train_feature_gen(train_data_df)
    pickle.dump(train_df,open(train_data_dump_path,'wb'),protocol=4)


    ##### test_Set
    test_data_df = pickle.load(open(merge_src_test_dump_path,'rb'))
    # print(test_data_df.columns)
    test_df = test_feature_gen(train_data_df,test_data_df)
    pickle.dump(test_df,open(test_data_dump_path,'wb'),protocol=4)