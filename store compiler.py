import pandas as pd
import numpy as np

df = pd.read_csv('store data.csv')

from StoreFunctions import store_stats,product_stats,customer_stats

store_stats(store_id=53,data=df)