# coding=gbk
import pandas as pd
import pickle
import os

filename = 'position.csv'
dirname = 'data/'
position_dump_path = 'saved_file/position.pkl'

def getPositionCategory():
    position_data = pd.read_csv(dirname + filename)
    if os.path.exists(position_dump_path):
        position = pickle.load(open(position_dump_path, 'rb'))
    else:
        #离散
        '''
        positionType = position_data['positionType']
        positionType_category = pd.get_dummies(positionType, prefix="postion")
        print(positionType_category)
        position_data = position_data.drop('positionType', axis=1)
        position = pd.concat([position_data, positionType_category], axis=1)
        '''
        position = position_data
        # 不离散
        pickle.dump(position_data, open(position_dump_path, 'wb'))

    # return position



#site只有２，３种，没必要统计
def getSiteIDCount():
    position_data = pd.read_csv(dirname + filename)
    position_groupby_siteID = position_data.groupby("sitesetID")
    postion_dataframe = position_groupby_siteID.count().reset_index()
    postion_dataframe = postion_dataframe.drop(['positionType','positionID'],axis = 1)
    postion_dataframe = postion_dataframe.rename(columns = {'positionID':'sitesetCount'},inplace = True)
    siteset_mean = postion_dataframe['sitesetCount'].mean()
    return postion_dataframe,siteset_mean

if __name__ == '__main__':
    filename = 'position.csv'
    #position_data = getdata(dirname + filename)
    getPositionCategory()
    #getSiteIDCount()