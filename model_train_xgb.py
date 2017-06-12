# coding=gbk
import numpy as np
import pandas as pd
import math
import compute_loss_and_save
import pickle
import xgboost as xgb
from sklearn.cross_validation import StratifiedKFold

# model training

train_dump_path = 'saved_file/merge_src_train_data.pkl'
test_dump_path = 'saved_file/merge_src_test_data.pkl'

train_data = pickle.load(open(train_dump_path,'rb'))
train_data = train_data.fillna(0)
train_label = train_data['label']
test_data = pickle.load(open(test_dump_path,'rb'))

train_data = train_data.drop(['label','conversionTime'], axis=1)
test_data = test_data.drop(['label','instanceID'], axis=1)
test_data.fillna(0)
# pkl_file = open('saved_file/merge_src_train_data.pkl','rb')
# train_info =  pickle.load(pkl_file)
# pkl_file.close()
day_30_list = train_data[train_data.click_day==30].index.tolist()

param = {}
# use softmax multi-class classification
param['objective'] = 'binary:logistic'              #����ࣺ'multi:softprob'
param['eval_metric '] = 'logloss' #У����������Ҫ������ָ��
param['eta'] = 0.1  #ͨ���������etaΪ0.01~0.2
# param['min_child_weight']=1 #���ӽڵ�����С������Ȩ�غ͡����һ��Ҷ�ӽڵ������Ȩ�غ�С��min_child_weight���ֹ��̽�����
# param['alpha '] =1 #Ĭ��0��L1����ͷ�ϵ����������ά�ȼ���ʱ����ʹ�ã�ʹ���㷨���и��졣
# param['lambda '] =1 #Ĭ��0��L2 ����ĳͷ�ϵ��
param['scale_pos_weight'] = 1 #Ĭ��0������0��ȡֵ���Դ������ƽ������������ģ�͸�������
param['max_depth'] = 6  #ͨ��ȡֵ��3-10
# param['colsample_bytree '] =0.8 #Ĭ��Ϊ1���ڽ�����ʱ��������������ı�����
# param['subsample']=0.8 #Ĭ��Ϊ1������ѵ��ģ�͵�������ռ�����������ϵı�����
# param['max_delta_step']=0.3  #ͨ������Ҫ�������ֵ������ʹ��logistics �ع�ʱ������𼫶Ȳ�ƽ�⣬������ò���������Ч��
param['silent'] = 1  #ȡ1ʱ��ʾ��ӡ������ʱ��Ϣ��ȡ0ʱ��ʾ�Լ�Ĭ��ʽ���У�����ӡ����ʱ����Ϣ��
# param['nthread'] = 4  #�����ϣ��������ٶ����У����鲻�������������ģ�ͽ��Զ��������߳�
#     param['num_class'] = 2  #�����ʱ������
# param['n_estimators'] = 100
# param['gamma'] = 0.1
# param['sample_type'] = 'uniform'
# param['normalize_type'] = 'forest'
# param['random_state'] = 1
num_round = 200 #���������ĸ���


# cv = StratifiedKFold(train_data, n_folds=6)  #6�۽���
valid_X = train_data[day_30_list[0]:]
valid_y = train_label[day_30_list[0]:]
train_data = train_data[0:day_30_list[0]]
train_label = train_label[0:day_30_list[0]]
# train_data = train_data[1000000:]
# train_label = train_label[1000000:]

#��30�յ���������ѵ��
# train_data = pd.concat([train_data,day_30_pos_df],join="inner",ignore_index=True)
# train_label = pd.concat([train_label,day_30_pos_label],join="inner",ignore_index=True)

print(train_data.columns,len(train_data))
xg_train = xgb.DMatrix( train_data, label=train_label)
xg_test = xgb.DMatrix(valid_X, label=valid_y)
watchlist = [ (xg_train,'train'), (xg_test, 'test') ]
bst = xgb.train(param, xg_train, num_round, watchlist,early_stopping_rounds=100)
probas_ = bst.predict( xg_test )
loss = compute_loss_and_save.logloss(valid_y,probas_)
print(loss)

testX = xgb.DMatrix(test_data)
proba_test = bst.predict( testX )
compute_loss_and_save.submission(proba_test)