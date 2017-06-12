# coding=gbk
import numpy as np
import pandas as pd
import pickle
import os

dirname = 'data/'
filename = 'user.csv'
user_dump_path = 'saved_file/user.pkl'

def convergAge(age):
    if age == 0:
        return 0
    elif age > 0 and age <= 16:
        return 1
    elif age > 16 and age <= 25:
        return 2
    elif age > 22 and age <= 35:
        return 3
    elif age > 35 and age <= 45:
        return 4
    elif age > 45 and age <= 55:
        return 5
    elif age > 55 and age <= 65:
        return 6
    else:
        return 7

def convergAddress(value):
    province = 0
    city = 0
    if value == 0:
        return province,city
    elif value >= 1000:
        province = value // 100
        city = value % 100
        return province,city
    elif value >= 100 and value < 1000:
        province = value // 100
        city = value % 100
        return province,city

def getUserCategory():
    user = pd.read_csv(dirname + filename)
    if os.path.exists(user_dump_path):
        pick_file = open(user_dump_path,'rb')
        user_data = pickle.load(pick_file)
        pick_file.close()
    else:
        user['age_hot'] = user['age'].map(convergAge)
        hometown = user['hometown'].apply(lambda x: convergAddress(x))
        home_dataframe = pd.DataFrame.from_records(hometown.tolist(),
                                                   columns=['hometown_province', 'hometown_city'])
        #home_province_category = pd.get_dummies(home_dataframe['home_province'],prefix='home_province')
        #home_city_category = pd.get_dummies(home_dataframe['home_city'],prefix='home_city')

        residence = user['residence'].apply(lambda x: convergAddress(x))
        residence_dataframe = pd.DataFrame.from_records(residence.tolist(),
                                                        columns=['residence_province', 'residence_city'])
        #residence_province = pd.get_dummies(residence_dataframe['residence_province'],prefix='residence_province')
        #residence_city = pd.get_dummies(residence_dataframe['residence_city'],prefix='residence_city')
        #离散
        '''
        age_code = pd.get_dummies(user['age'], prefix="age")
        gender_code = pd.get_dummies(user['gender'], prefix="gender")
        education_code = pd.get_dummies(user['education'], prefix="education")
        marriage_code = pd.get_dummies(user['marriageStatus'],prefix="marriage")
        haveBady = pd.get_dummies(user['haveBaby'], prefix="haveBady")
        hometown_province = pd.get_dummies(home_dataframe['hometown_province'], prefix="hometown_province")
        hometown_city = pd.get_dummies(home_dataframe['hometown_city'], prefix="hometown_city")
        residence_province = pd.get_dummies(residence_dataframe['residence_province'], prefix="residence_province")
        residence_city = pd.get_dummies(residence_dataframe['residence_city'], prefix="residence_city")

        user_data = pd.concat([user['userID'], age_code, gender_code, education_code, marriage_code, haveBady,hometown_province,
                hometown_city, residence_province,residence_city],axis=1)
        '''
        #不离散
        user_data = pd.concat([user['userID'],home_dataframe,residence_dataframe,
                               user['age'],user['age_hot'],user['gender'],user['education'],
                               user['marriageStatus'],user['haveBaby']],axis= 1)
        output = open(user_dump_path,'wb')
        pickle.dump(user_data,output)
        output.close()
    return user_data


if __name__ == '__main__':
    getUserCategory()
