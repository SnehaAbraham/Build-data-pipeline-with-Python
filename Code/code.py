# -*- coding: utf-8 -*- 
"""
Created on Fri Mar 19 16:28:22 2021

@author: Sneha Abraham
"""

import psycopg2
import pandas as pd
import timeit
import numpy as np
import matplotlib.pyplot as plt
#import getpass
import logging
#import sys
#import datetime

logging.basicConfig(level=logging.DEBUG)

###Establish connection to psycopg2##########################################  
con = psycopg2.connect(
    	host="kashin.db.elephantsql.com",
        database="mvrrimnt",
        user="mvrrimnt",
        password="8ptJndb-lYyvEYIhgYvbB9AHhu1UwtoR")
cur = con.cursor()
    
###Get Inputdata from file###################################################
def create_pandas_table(sql_query, database = con):
    table = pd.read_sql_query(sql_query, database)
    return table

activity = create_pandas_table("select * from activity")
members = create_pandas_table("select * from members")
members.to_csv('members1.csv')
activity.to_csv('activity.csv')
#########Merge both the dataframes activities & members to details of signup_date & transaction date
df = pd.merge(members, activity, on='user_id')
df.to_csv('data.csv', index=False)


def execute(df):
    time_start = timeit.default_timer()
    ###Find user_id greater than 1########################################
    df1 = df[df.groupby('user_id')['user_id'].transform('size') > 1]
    df1.to_csv('df1.csv')
    ###Setting standard datatype##########################################
    df1['signup_date']=df1['signup_date'].astype('datetime64[ns]')
    df1['act_timestamp']=df1['act_timestamp'].astype('datetime64[ns]')
    ###Finding diff between two dates i.e signup_date & act_timestamp#####
    df1['Diff'] = (abs(df1['signup_date'] - df1['act_timestamp']))
    df1['Diff']=df1['Diff'] / np.timedelta64(1, 'D')
    ###Apply only Activated where date_diff=7#############################
    df1['members_activation'] = df1['Diff'].apply(lambda x: "Activated" 
                                        if x==7 else "Not-Activated")
    output=df1[['user_id','signup_date','channel','members_activation']]
    output.to_csv('Results.csv', index=False)
    output_final=df1[['user_id','signup_date','channel','act_timestamp',
                     'act_type','Diff','members_activation']]
    output_final.to_csv('Results_final.csv', index=False)
    time_end = timeit.default_timer()
    logging.debug('Time to finish processing data:'+str(time_end - time_start))
    
    cur.execute('DELETE FROM members_copy')
    cur.execute('ALTER TABLE members_copy ADD COLUMN IF NOT EXISTS members_activation text NOT NULL')

    with open('Results.csv','r') as f:
        f.readline()
        cur.copy_from(f, 'members', sep=',')
        con.commit()
        cur.close()
        con.close()

#######Run Script#############################################################
execute(df)
   


        
        
    
    
    





