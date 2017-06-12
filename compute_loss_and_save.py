# coding=gbk
import zipfile
import pandas as pd
import scipy as sp

def logloss(act, pred):
    epsilon = 1e-15
    pred = sp.maximum(epsilon, pred)
    pred = sp.minimum(1-epsilon, pred)
    ll = sum(act*sp.log(pred) + sp.subtract(1,act)*sp.log(sp.subtract(1,pred)))
    ll = ll * -1.0/len(act)
    return ll

# submission
def submission(pred):
    dfTest = pd.read_csv("data/test.csv")
    print(len(dfTest['instanceID']))
    print(len(pred))
    df = pd.DataFrame({"instanceID": dfTest["instanceID"].values, "proba": pred})
    df.sort_values("instanceID", inplace=True)
    df.to_csv("saved_file/submission.csv", index=False)  #path_or_buf="data/sumission.csv",
    with zipfile.ZipFile("submission.zip", "w") as fout:
        fout.write("saved_file/submission.csv", compress_type=zipfile.ZIP_DEFLATED)