#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from airflow.models import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime
import pandas as pd
import numpy as np

######################################################################
def store_stats (store_id,data):
    
    df = data
    ## Filter for store id
    
    aggregator = 'store_id'
    
    df_filt = df[df[aggregator]==store_id]
    
    ## Customer count
    customer_count = df_filt['customer_id'].nunique()
    customer_count = [customer_count]
    
    ## Total interactions
    total_transactions = df_filt['date'].count()
    total_transactions = [total_transactions]
    
    ## Average interactions
    average_transactions = round((df_filt['date'].count() / df_filt['customer_id'].nunique()),2)
    average_transactions = [average_transactions]
  
    ## Convert store id to list
    store_identifier = store_id
    store_id = [store_id]
  
    ## Create data frame
    store_stats = pd.DataFrame({'store_id':store_id,'customer_count':customer_count,'total_transactions':total_transactions,'average_transactions':average_transactions})
   
   
    ## Export dataset
    store_stats.to_csv('/home/airflow/airflow/data/store stats store {}.csv'.format(store_identifier),index=False)   

    ######################
def product_stats (store_id,data):
    
    df = data
    ## Filter for store id
    aggregator = 'store_id'

    df_filt = df[df[aggregator]==store_id]

    ## customers per product
    customers = df_filt.groupby('product_id')['customer_id'].nunique()

    ## transactions per product
    transactions = df_filt.groupby('product_id')['time_stamp'].count()

    ### average transactions per product
    average_transactions = round((df_filt.groupby('product_id')['time_stamp'].count() / df_filt.groupby('product_id')['customer_id'].nunique() ),2)

    ## Create data frame
    product_stats = pd.DataFrame({'customers':customers,'transactions':transactions,'average_transactions':average_transactions})

    product_stats['store_id'] = store_id

    ## Export dataset
    product_stats.to_csv('/home/airflow/airflow/data/product stats store {}.csv'.format(store_id),index=False)

    #############
def customer_stats(data):
    
    import pandas as pd
    
    df = data
    ## Filter for store id

    ## total transactions
    total_transactions = df.groupby('customer_id')['time_stamp'].count()

    ## days visited
    days = df.groupby('customer_id')['date'].nunique()

    ## months visited
    months = df.groupby('customer_id')['month_year'].nunique()


    ## Create data frame
    customer_stats = pd.DataFrame({'transactions':total_transactions,'days_visited':days,'months_visited':months})

    ## Export dataset
    customer_stats.to_csv('/home/airflow/airflow/data/customer stats.csv',index=False)

####################################################################

## import data
df = pd.read_csv('/home/airflow/airflow/data/store data.csv')
    
## Dag and tasks

default_args = {
    'start_date' : datetime(2021,7,1)
    }

with DAG ('store_processing',schedule_interval='@daily',default_args=default_args,catchup=False) as dag:
    
    ## store 53
    store_process_53 = PythonOperator(
        task_id='store_process_53',
        python_callable = store_stats,
        op_kwargs = {'store_id':53,'data':df}
        )
    
    ## store 55
    store_process_55 = PythonOperator(
        task_id='store_process_55',
        python_callable = store_stats,
        op_kwargs = {'store_id':55,'data':df}
        )
    
    ## store 82
    store_process_82 = PythonOperator(
        task_id='store_process_82',
        python_callable = store_stats,
        op_kwargs = {'store_id':82,'data':df}
        )

    ## product 53
    product_process_53 = PythonOperator(
        task_id='product_process_53',
        python_callable = product_stats,
        op_kwargs = {'store_id':53,'data':df}
        )
    
    ## product 55
    product_process_55 = PythonOperator(
        task_id='product_process_55',
        python_callable = product_stats,
        op_kwargs = {'store_id':55,'data':df}
        )

    ## product 82
    product_process_82 = PythonOperator(
        task_id='product_process_82',
        python_callable = product_stats,
        op_kwargs = {'store_id':82,'data':df}
        )

    ## customer
    customer_process = PythonOperator(
        task_id='customer_process',
        python_callable = customer_stats,
        op_kwargs = {'data':df}
        )

    store_process_53 >> store_process_55 >> store_process_82 >> product_process_53 >> product_process_55 >> product_process_82 >> customer_process

