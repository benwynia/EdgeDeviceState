#!/usr/bin/env python
# coding: utf-8

# <b> Get Building Information </b>

# In[6]:


# Import Libraries
from pymongo import MongoClient
from bson.json_util import dumps, loads
import datetime
import json
import ssl
import pandas as pd
pd.options.mode.chained_assignment = None
import connect_to_mongo
import parse_mac_address


# In[9]:


def get_nwc_command_state(client):
    from_date_datetime = datetime.datetime.utcnow() + datetime.timedelta(minutes=-5)
    
    # Define the query
    query_mongodb = [
        { "$match": { "created_at": { "$gte": from_date_datetime} } },        
        {  "$group": {
                "_id": "$vendor_id",
                "object_id":
                    {"$last": "$object_id"},
                "state":
                    {"$last": "$state"},
                "command":
                    {"$last": "$command"},
                "lite_id":
                    {"$last": "$lite_id"},
                "fw_version":
                    {"$last": "$fw_version"},
                "read_error":
                    {"$last": "$read_error"},
                "created_at":
                    {"$last": "$created_at"},
                }
        },
        {"$sort": {"created_at": -1}}
    ]

    # Create the database cursor
    db = client['network_window_controller']

    # Call query and write result
    data_json = db.timeseries_data.aggregate(query_mongodb)

    # Convert data_json to a pandas dataframe
    df = pd.DataFrame(list(data_json))
    
    # Minor table modifications
    df['wc_id'] = df['object_id']
    
    df['mac_address'] = [parse_mac_address.format_mac(x) for x in df['_id']]
    df['wc_id'] = df['object_id']
    df=df[["wc_id","_id","mac_address", "lite_id","fw_version","command","state","read_error","created_at"]]
    return(df)


# In[10]:


if __name__ == '__main__':
    hostname="100.96.128.93"
    client = connect_to_mongo.connection(hostname)
    nwc_command_state = get_nwc_command_state(client)




