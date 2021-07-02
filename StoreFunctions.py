## Store Stats Function
def store_stats (store_id,data):

    import pandas as pd
    import numpy as np
    
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
    store_stats.to_csv('D:/airflow project/store stats store {}.csv'.format(store_identifier),index=False)    
    


## Product stats function
def product_stats (store_id,data):

    import pandas as pd
    import numpy as np
    
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
    product_stats.to_csv('D:/airflow project/product stats store {}.csv'.format(store_id),index=False)


## Customer stats function
def customer_stats(data):
    
    import pandas as pd
    import numpy as np
    
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
    customer_stats.to_csv('D:/airflow project/customer stats.csv',index=False)


































  
