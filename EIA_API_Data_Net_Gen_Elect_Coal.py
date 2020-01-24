#!/usr/bin/env python
# coding: utf-8

# In[158]:


import eia


# In[159]:


import pandas as pd


# In[160]:


import sqlalchemy


# In[161]:


from sqlalchemy import create_engine


# In[162]:


import psycopg2


# In[163]:


import datetime


# In[164]:


import os


# In[165]:


import sys


# In[166]:


(r"C:\Users\ghoelzle\db_access")


# In[167]:


states = ["AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD","DC", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY","PR"]


# In[175]:



def retrieve_time_series(api, series_ID):
    series_search = api.data_by_series(series=series_ID)
    df = pd.DataFrame(series_search)
    return df
    
c_df = pd.DataFrame()
    
for state in states:
    #Create EIA API using your specific API key
    api_key = "b0ab3c2d89e5aacbfff2f6eeec135555"
    print(api_key)
    api = eia.API(api_key)
    #Declare desired series ID
    try:
        series_ID=('ELEC.GEN.COW-{}-1.M').format(state)
        df=retrieve_time_series(api, series_ID)
        df.rename(columns={ df.columns[0]: "Thousand MWH" }, inplace = True)
        df['stusps10']=state
        df["Technology"]='Coal'
        df.reset_index(level=0, inplace=True)
        df.rename(columns={ df.columns[0]: 'Date' }, inplace = True)
        c_df = c_df.append(df)
        del df
            
    except:
        print('State ' + state + ' failed')
        continue
    ##Run Monthly##
    
c_df = c_df.reset_index(drop=True)
    


# In[177]:


def db_connect():
    db_connect_string = "postgresql+psycopg2://{user}:{passwd}@{server}:{port}/{db}".format(user="postgres", passwd="postgres",
    server="141.211.55.211", db="fptz", port="58425")
    ssl_args = {
        "sslmode": "require",
        "sslcert": "postgresql.crt",
        "sslkey": "postgresql.key",
        "sslrootcert": "root.crt",
    }
    return create_engine(db_connect_string, connect_args=ssl_args)


# In[178]:


db_connect_string = "postgresql+psycopg2://{user}:{passwd}@{server}:{port}/{db}".format(user="postgres", passwd="postgres",
    server="141.211.55.211", db="fptz", port="58425")


# In[179]:


ssl_args = {
        "sslmode": "require",
        "sslcert": "postgresql.crt",
        "sslkey": "postgresql.key",
        "sslrootcert": "root.crt",}


# In[180]:


engine = create_engine(db_connect_string, connect_args= ssl_args)


# In[181]:


con = engine.connect


# In[182]:


c_df.to_sql("net_generation_coal_electric_utility_monthly",engine,if_exists="replace")


# In[ ]:




