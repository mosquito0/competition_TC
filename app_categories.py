# coding=gbk                                
import pandas as pd
import pickle
import os

filename = 'app_categories.csv'
dirname = 'data/'
app_dump_path = 'saved_file/app_category.pkl'

def convertother(value):
    '''
    :param value:int
    :return: dataframe
    '''
    firstClass = 0
    secondClass = 0
    if value == 0:
        return firstClass, secondClass
    elif value >= 100:
        firstClass = value // 100
        secondClass = value % 100
        return firstClass, secondClass
    elif value >=0 and value < 100:
        firstClass = value
        return firstClass, secondClass

def getAPPCatagory():
    app_data = pd.read_csv(dirname + filename)
    if os.path.exists(app_dump_path):
        appCategory_total = pickle.load(open(app_dump_path, 'rb'))
        appCategory = appCategory_total[0]
        category_mean = appCategory_total[1]
        #print(appCategory)
    else:

        app_groupby_category = app_data.groupby('appCategory')
        amount = app_groupby_category.count().reset_index()
        amount.rename(columns={'appID':'appCategory_count'},inplace = True)
        category_mean = amount[amount['appCategory']!=0]['appCategory_count'].mean()

        appCategory = app_data['appCategory']  #series

        category = appCategory.apply(lambda x:convertother(x))
        category_dataframe = pd.DataFrame.from_records(category.tolist(),columns=['appCategoryFirstClass','appCategorySecondClass'])

        #appCategory_data = app_data.drop('appCategory', axis=1) #先不要删掉这列
        appCategory = pd.concat([app_data, category_dataframe], axis=1)
        appCategory = pd.merge(appCategory,amount,how='left',on='appCategory')
        appCategory = appCategory.drop(['appCategory'],axis = 1)

        #离散
        '''
        FirstClass_dummy = pd.get_dummies(appCategory['appCategoryFirstClass'],prefix='appCategoryFirstClass')
        SecondeClass_dummy = pd.get_dummies(appCategory['appCategorySecondClass'],prefix='appCategorySecondClass')
        appCategory = pd.concat([appCategory,FirstClass_dummy,SecondeClass_dummy],axis= 1)
        appCategory = appCategory.drop(['appCategoryFirstClass','appCategorySecondClass'],axis = 1)
        '''
        #不离散(直接生成文件就行)
        output = open(app_dump_path,'wb')
        pickle.dump(appCategory,output)
        output.close()
        # pickle.dump([appCategory,category_mean], open(app_dump_path, 'wb'))

    # return appCategory,category_mean

#APP安装比例
def getAPPInstallratio():

    user_install_app_data = pd.read_csv(dirname + 'user_installedapps.csv')
    user_app_action_data = pd.read_csv(dirname + 'user_app_actions.csv')
    data = pd.merge(user_app_action_data,user_install_app_data,how='outer',on='appID')
    print(data)
    for day in range(17,31):
        pass
    pass

if __name__ == '__main__':
    '''
    filename = 'app_categories.csv'
    app_data = getdata(dirname + filename)
    '''
    getAPPCatagory()
