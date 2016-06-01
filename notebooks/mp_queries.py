# Import libraries
import numpy as np
import pandas as pd
import psycopg2
import sys
import datetime as dt




def query_metavision_patients(iid, sqluser='alistairewj', dbname='mimic',schema_name='mimiciii'):

# Connect to local postgres version of mimic
con = psycopg2.connect(dbname=dbname, user=sqluser)
cur = con.cursor()
cur.execute('SET search_path to ' + schema_name)

query = \
"""
select icustay_id, dbsource
from icustays
"""
db = pd.read_sql_query(query,con)
cur.close()
con.close()

db.set_index('icustay_id',inplace=True)

return db.loc[db['dbsource']=='metavision',:].index.values


def query_infusions(iid, sqluser='alistairewj', dbname='mimic',schema_name='mimiciii'):

# Connect to local postgres version of mimic
con = psycopg2.connect(dbname=dbname, user=sqluser)
cur = con.cursor()
cur.execute('SET search_path to ' + schema_name)

if 'float' in str(type(iid)) or 'int' in str(type(iid)):
    iid_str = str(iid)
else:
    iid_str = iid

query = \
"""
select icustay_id, dbsource
from icustays
where icustay_id = """ + iid_str
db = pd.read_sql_query(query,con)
db.set_index('icustay_id',inplace=True)
db = db.loc[iid,'dbsource']

if db != 'metavision':
    print('Cannot extract inputs for {} data.'.format(db))
    return None


query = \
"""
select
    mv.icustay_id
    , starttime - ie.intime AS icustarttime
    , endtime - ie.intime AS icuendtime
    , di.label, amount, amountuom, rate, rateuom
    , orderid, linkorderid
from inputevents_mv mv
inner join icustays ie
    on mv.icustay_id = ie.icustay_id
inner join d_items di
    on mv.itemid = di.itemid
where mv.icustay_id = """ + iid_str + """
order by mv.icustay_id, starttime, endtime, orderid
"""

df = pd.read_sql_query(query,con)
cur.close()
con.close()

return df


def query_codestatus(iid, sqluser='alistairewj', dbname='mimic',schema_name='mimiciii'):

# Connect to local postgres version of mimic
con = psycopg2.connect(dbname=dbname, user=sqluser)
cur = con.cursor()
cur.execute('SET search_path to ' + schema_name)

if 'float' in str(type(iid)) or 'int' in str(type(iid)):
    iid_str = str(iid)
else:
    iid_str = iid

query = \
"""
select icustay_id, dbsource
from icustays
where icustay_id = """ + iid_str
db = pd.read_sql_query(query,con)
db.set_index('icustay_id',inplace=True)
db = db.loc[iid,'dbsource']

if db != 'metavision':
    print('Cannot extract inputs for {} data.'.format(db))
    return None


query = \
"""
select
    ce.icustay_id
    , charttime - ie.intime AS icutime
    , di.label, ce.value
from chartevents ce
inner join icustays ie
    on ce.icustay_id = ie.icustay_id
inner join d_items di
    on ce.itemid = di.itemid
where ce.itemid in (128, 223758)
and ce.icustay_id = """ + iid_str + """
and ce.error != 1
order by ce.icustay_id, icutime
"""

df = pd.read_sql_query(query,con)
cur.close()
con.close()

return df

def query_charts(iid, sqluser='alistairewj', dbname='mimic',schema_name='mimiciii'):

# Connect to local postgres version of mimic
con = psycopg2.connect(dbname=dbname, user=sqluser)
cur = con.cursor()
cur.execute('SET search_path to ' + schema_name)

if 'float' in str(type(iid)) or 'int' in str(type(iid)):
    iid_str = str(iid)
else:
    iid_str = iid

query = \
"""
select icustay_id, dbsource
from icustays
where icustay_id = """ + iid_str
db = pd.read_sql_query(query,con)
db.set_index('icustay_id',inplace=True)
db = db.loc[iid,'dbsource']

if db != 'metavision':
    print('Cannot extract inputs for {} data.'.format(db))
    return None

# Load chartevents
query = """
select
    ce.icustay_id
    , ce.charttime
    , charttime - ie.intime AS icutime
    , di.label, ce.value
    , ce.valuenum, ce.valueuom
from chartevents ce
inner join icustays ie
    on ce.icustay_id = ie.icustay_id
inner join d_items di
    on ce.itemid = di.itemid
where ce.icustay_id = """ + iid_str + """
and error != 1
order by ce.icustay_id, icutime
"""

charts = pd.read_sql_query(query,con)
cur.close()
con.close()

return charts
