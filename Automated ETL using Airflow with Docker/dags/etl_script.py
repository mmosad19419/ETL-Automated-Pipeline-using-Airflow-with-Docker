# import libs
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

import pandas as pd
import numpy as np
import os
import glob

from sklearn.preprocessing import OrdinalEncoder
from sklearn.preprocessing import StandardScaler

# define extract and integration function
def extract_integrate(filepath):
    #  get all the csv files in the data dir
    all_files = []

    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, "*.csv"))

        for f in files:
            all_files.append(os.path.abspath(f))
    
    print("got file paths successfully")
    
    # define dfs list to save df 
    dfs = list()

    for file in all_files :
        df = pd.read_csv(file)
        dfs.append(df)
    print("loaded CSVs files to df successfully")
    
    # apply accumulative integration
    # load the first df
    df_integrated = dfs[0]

    for i in range(0, len(dfs)):
        if i == 0:
            continue
        
        df = dfs[i][["accident_reference", "age_of_driver"]]

        # integrate dfs
        df_integrated = df_integrated.merge(df, on ="accident_reference", how = "inner",
          suffixes=(None, '_DROP')).filter(regex='^(?!.*_DROP)')
    
    # output to csv
    df_integrated.to_csv('/opt/airflow/data/accidents_integrated.csv',index=False)
    print("data integrated successfully")


# define clean functions
def data_clean(filename):
    # load integrated data
    df = pd.read_csv(filename)

    # drop mask
    mask_drop1 = df.select_dtypes(object).nunique() > 2000
    mask_drop2 = df.select_dtypes(object).nunique() == 1

    # select the columns to drop
    drop1 = df.select_dtypes(object).loc[:,mask_drop1].columns
    drop2 = df.select_dtypes(object).loc[:,mask_drop2].columns

    # drop the selected columns
    df.drop(df[drop1].columns , axis =1,inplace = True)
    df.drop(df[drop2].columns , axis =1,inplace = True)

    #replace the remaining value with mode
    for i in df.loc[:,df.isnull().sum() > 0].columns:
        df[i] = df[i].fillna(df[i].mode()[0])

    # road Classification columns [A, B, C, motorway]
    # -1 represent unclassified road
    df.first_road_class.replace("-1", "Unclassified", inplace 	= True)
    df.second_road_class.replace("-1", "Unclassified", inplace 	= True)

    # A(M) represent motorway roads class
    df.first_road_class.replace("A(M)", "Motorway", inplace = 	True)
    df.second_road_class.replace("A(M)", "Motorway", inplace = 	True)


    # replace "Data missing or out of range" values by Nan(MCAR) and drop
    other_null_values = ["Data missing or out of range"]
    df.replace(other_null_values, np.nan, inplace = True)
    df.dropna(inplace = True)

    # determine the IQR
    Q1 = df['age_of_driver'].quantile(0.25)
    Q3 = df['age_of_driver'].quantile(0.75)
    IQR = Q3 - Q1
    cut_off = IQR * 1.5
    lower = Q1 - cut_off
    upper =  Q3 + cut_off

    # impute outliers
    df['age_of_driver'] = df['age_of_driver'].apply(lambda x : impute_age_outliers(x, upper))

    
    # output cleaned data
    df.to_csv('/opt/airflow/data/accidents_cleaned.csv',index=False)
    print("Data Cleaned Successfully")


def transform_encode_load(filename):
    try:
        df = pd.read_csv(filename)
        print('loaded for peprocessing succesfully')
    except FileExistsError:
        print('file does not exists')

    # create  week number column
    df['Week_Number'] = pd.to_datetime(df['date']).dt.isocalendar().week

    # create week end column feature
    df['Week_end'] = df['day_of_week'].apply(week_end)

    # create feature to represent accidents happen in winter cndition
    df['winter_condition'] = df['road_surface_conditions'].apply(winter_condtion)

    # there is some cols need to be normalzies meaning full cols with skewness more than .5
    normalize_cols = ['number_of_vehicles','number_of_casualties','speed_limit']
    for col in normalize_cols:
        df[col] = np.log1p(df[col])
       
    #converting date to seperate cols 
    df['date'] = pd.to_datetime(df['date'])
    df['month'] = df.date.dt.month
    df['day'] = df.date.dt.day
   
    # drop un required columns
    df.drop(['date','accident_year'],axis = 1 , inplace = True)

    # add the bins to reresent accident time 
    labels = ['00:00-05:59', '06:00-11:59', '12:00-17:59', '18:00-23:59']
    bins = [0, 6, 12, 18, 24]

    df['Time Bin'] = pd.cut(pd.to_datetime(df.time, format = "%H:%M").dt.hour, bins, labels=labels, right=False)
    df.drop('time',axis = 1, inplace = True)

    #we will ordinal encode first road class and second road class the resest of them will one hot encoding 
    encoder = OrdinalEncoder()
    df['first_road_class'] = encoder.fit_transform(np.array(df['first_road_class']).reshape(409623 ,-1))
    df['second_road_class'] = encoder.fit_transform(np.array(df['second_road_class']).reshape(409623 ,-1))
  
    df_encoded = pd.get_dummies(df)

    # scale the data
    sc = StandardScaler()
    df_encoded[['number_of_vehicles', 'number_of_casualties', 'speed_limit', 'age_of_driver']]=sc.fit_transform(df_encoded[['number_of_vehicles','number_of_casualties','speed_limit', 'age_of_driver']])

    # outut the transformed data
    print("Data preprocessed successfully")
    df_encoded.to_csv('/opt/airflow/data/accidents_preprocessed.csv', index=False)


# def helper function
def week_end(x):
    if x in ["Saturday",'Sunday']:
        return 1
    else:
        return 0


# def helper function
def winter_condtion(x):
    if x in ['Frost or ice','Snow']:
        return 1
    else:
        return 0


# def helper impute function
# impute values higher 72 by 72
# impute values lower 18 by 18
def impute_age_outliers(x, upper):
    if x > upper:
        x = upper
        return x
    
    elif x < 18:
        x = 18
        return x
    else:
        return x 