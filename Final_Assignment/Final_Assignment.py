import numpy as np
import pandas as pd
import dask.dataframe as dd
import dask.array as da
from dask_ml.preprocessing import OneHotEncoder
from dask_ml import model_selection
import sklearn
from sklearn.ensemble import RandomForestClassifier
from sklearn import metrics 
from dask.distributed import Client
from dask_ml.wrappers import ParallelPostFit
import joblib
import time
import pickle
from scipy.sparse import csr_matrix


# get the start time
st = time.time()
 
#path='/data/dataprocessing/interproscan/all_bacilli.tsv'
path='/students/2021-2022/master/Dilton_DSLS/part.tsv'# 1million lines

def read_clean(path):
    """ Function to read the data and some data processing
    Arguments:
        path: path where the file is located as string
       
    Return:
        ddf: dask dataframe
    """
    ddf = dd.read_csv(path,sep='\t',usecols=[0,2,6,7,11],header=None,blocksize="50MB")

    #Renaming the columns
    ddf=ddf.rename(columns={0: "Protein",2:'Sequence_length',6:'Start',7:'Stop',11:'Interpro_accession',}) 
    
    #deleting the null interpro accessions
    ddf=ddf.loc[~(ddf['Interpro_accession']=='-')]

    #dropping duplicates
    ddf=ddf.drop_duplicates()

    #creating feat length column
    ddf['feat_len']=(ddf.Stop-ddf.Start)/ddf['Sequence_length']
    return ddf

def fun1(x):
    """ Function to check whether an Interpro accession is large or small.Used with apply method
    Arguments:
        x: dask dataframe  
    Return:
        0: if Interpro accession is small
        1: if Interpro accession is large
    """
    if x['feat_len']>.90:
        return 1
    else:
        return 0


def find_size(x):
    """ Function that returns proteins with atleast one small and large interpro accession.
        Used with groupby apply method
    Arguments:
        x:groupby object grouped on protein
    Return:
        x:Returns the datframe
    """
    m=np.mean(x['size'])
    
    if (m>0) and (m<1) : 
        return x

def final_df(s_pivot,l_df):
    """ Function to merge large dataframe with small dataframe on protein.
    Arguments:
        s_pivot: Pivoted dataframe in which columns are the name of small interpro accession 
                 and values are their counts
        l_df:  Dask datframe containing the largest interpro accession of a protein
    Return:
        f_df: Final dataframe for machine learning
    """
    f_df=s_pivot.merge(l_df,how='inner',left_on='Protein',right_on='Protein')
    f_df=f_df.drop(columns=['feat_len'])
    f_df=f_df.replace(np.nan, 0)
    f_df=f_df.reset_index()
    return f_df

## Machine learning

def ml_dfs(f_df):
    """ Function to perform one hot encoding on class labels and to do train test splitting
    Arguments:
        f_df: Dask dataframe with the count of small interpro accession and 
              the name of largest interpro accession of each protein
    Return:
        X_train,y_train: Training dask arrays
        X_test,y_test: Testing dask arrays 
    """
    y = OneHotEncoder().fit_transform(f_df[['Interpro_accession']])
    X=f_df.iloc[:,2:-1].to_dask_array(lengths=True)
    
    X_train, X_test, y_train, y_test = model_selection.train_test_split(X, y, test_size = 0.2,random_state=42,convert_mixed_types=True)

    return X_train, X_test, y_train, y_test


if __name__ == "__main__":
    ddf=read_clean(path)

    ddf['size']=ddf.apply(lambda x:fun1(x),axis=1)

    #creating dataframe with proteins which have atleast one big and small interpro accession
    full_df=ddf.groupby(['Protein',]).apply(find_size,meta={'Protein':'str','Sequence_length':'int64','Start':'int64',
'Stop':'int64','Interpro_accession':'str','feat_len':'int64','size':'int64',}).reset_index(drop=True)
    
    #splittng dataframe into two containing large and small interpro accessions
    large_df=full_df.loc[full_df['size']==1]
    small_df=full_df.loc[full_df['size']==0]

    client = Client()
    with joblib.parallel_backend('dask'):

        #Finding the largest interpro accession of a protein
        l_df = large_df.loc[large_df.groupby(['Protein'])['feat_len'].transform(max) == large_df['feat_len'],['Protein','Interpro_accession','feat_len',]]
        #counting the number of each small interpro accession#transform('count')#agg('count').reset_index()
        s_df=small_df.groupby(['Protein','Interpro_accession'])['size'].agg('count').reset_index()

        #catego
        s_df=s_df.categorize(columns=['Interpro_accession'])
        l_df=l_df.categorize(columns=['Interpro_accession'])

        s_pivot=dd.reshape.pivot_table(s_df,index='Protein',columns='Interpro_accession', values='size')

        f_df=final_df(s_pivot,l_df)

        X_train, X_test, y_train, y_test=ml_dfs(f_df)

    mid_time=(time.time() - st)/60
    print('Entering ML part', mid_time, 'minutes')


    with joblib.parallel_backend('dask'):
        # creating randomforestclassifier object
        clf = RandomForestClassifier(random_state=0)
        clf.fit(X_train, y_train)
        # performing predictions on the test dataset
        y_pred = clf.predict(X_test)
        # using metrics module for accuracy calculation
        accuracy=metrics.accuracy_score(y_test, y_pred)
        
        filename = '/students/2021-2022/master/Dilton_DSLS/randforest_model.pkl'
        pickle.dump(clf, open(filename, 'wb'))
        
        X_df=dd.from_dask_array(X_train,columns=f_df.columns[2:-1])

        X_df.to_csv('/students/2021-2022/master/Dilton_DSLS/X_train.csv')

        print("ACCURACY OF THE MODEL: ", accuracy)
        
    # get the end time
    et = time.time()

    # get the execution time
    elapsed_time = (et - st)/60
    print('Execution time:', elapsed_time, 'minutes')


