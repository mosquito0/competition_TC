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
param['objective'] = 'binary:logistic'              #多分类：'multi:softprob'
param['eval_metric '] = 'logloss' #校验数据所需要的评价指标
param['eta'] = 0.1  #通常最后设置eta为0.01~0.2
# param['min_child_weight']=1 #孩子节点中最小的样本权重和。如果一个叶子节点的样本权重和小于min_child_weight则拆分过程结束。
# param['alpha '] =1 #默认0，L1正则惩罚系数，当数据维度极高时可以使用，使得算法运行更快。
# param['lambda '] =1 #默认0，L2 正则的惩罚系数
param['scale_pos_weight'] = 1 #默认0，大于0的取值可以处理类别不平衡的情况。帮助模型更快收敛
param['max_depth'] = 6  #通常取值：3-10
# param['colsample_bytree '] =0.8 #默认为1，在建立树时对特征随机采样的比例。
# param['subsample']=0.8 #默认为1，用于训练模型的子样本占整个样本集合的比例。
# param['max_delta_step']=0.3  #通常不需要设置这个值，但在使用logistics 回归时，若类别极度不平衡，则调整该参数可能有效果
param['silent'] = 1  #取1时表示打印出运行时信息，取0时表示以缄默方式运行，不打印运行时的信息。
# param['nthread'] = 4  #如果你希望以最大速度运行，建议不设置这个参数，模型将自动获得最大线程
#     param['num_class'] = 2  #多分类时需设置
# param['n_estimators'] = 100
# param['gamma'] = 0.1
# param['sample_type'] = 'uniform'
# param['normalize_type'] = 'forest'
# param['random_state'] = 1
num_round = 200 #提升迭代的个数


# cv = StratifiedKFold(train_data, n_folds=6)  #6折交叉
valid_X = train_data[day_30_list[0]:]
valid_y = train_label[day_30_list[0]:]
train_data = train_data[0:day_30_list[0]]
train_label = train_label[0:day_30_list[0]]
# train_data = train_data[1000000:]
# train_label = train_label[1000000:]

#将30日的正例加入训练
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