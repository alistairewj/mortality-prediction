# Import libraries
import numpy as np
import pandas as pd
import psycopg2
import sys
import datetime as dt
from sklearn import metrics

# pretty confusion matrices!
def print_cm(y, yhat):
    print('\nConfusion matrix')
    cm = metrics.confusion_matrix(y, yhat)
    TN = cm[0,0]
    FP = cm[0,1]
    FN = cm[1,0]
    TP = cm[1,1]
    N = TN+FP+FN+TP
    print('   \t{:6s}\t{:6s}').format('yhat=0','yhat=1')
    print('y=0\t{:6g}\t{:6g}\tSpec={:2.2f}').format(cm[0,0],cm[0,1], 100.0*TN/(TN+FP)) # Spec
    print('y=1\t{:6g}\t{:6g}\tSens={:2.2f}').format(cm[1,0],cm[1,1], 100.0*TP/(TP+FN)) # Sens
    # add sensitivity/specificity as the bottom line
    print('   \t{:2.2f}\t{:2.2f}\t Acc={:2.2f}').format(100.0*TN / (TN+FN), 100.0*TP / (TP+FP), 100.0*(TP+TN)/N)
    print('   \tNPV\tPPV')

# these define 5 lists of variable names
# these are the variables later used for prediction
def vars_of_interest():
    # we extract the min/max for these covariates
    var_min = ['heartrate', 'systolicbp', 'diastolicbp', 'meanbp',
               'resprate', 'temp', 'spo2', 'glucosecharted']
    var_max = var_min
    #var_max.extend(['rrt','vasopressor','vent'])
    var_min.append('gcs')


    # we extract the first/last value for these covariates
    var_first = ['heartrate', 'systolicbp', 'diastolicbp', 'meanbp',
                'resprate', 'temp', 'spo2', 'glucosecharted']

    var_last = var_first
    var_last.extend(['gcsmotor','gcsverbal','gcseyes','endotrachflag','gcs'])

    var_first_early = ['bg_so2', 'bg_po2', 'bg_pco2', #'bg_fio2_chartevents', 'bg_aado2_calc',
            'bg_fio2', 'bg_aado2', 'bg_pao2fio2', 'bg_ph', 'bg_baseexcess', 'bg_bicarbonate',
            'bg_totalco2', 'bg_hematocrit', 'bg_hemoglobin', 'bg_carboxyhemoglobin', 'bg_methemoglobin',
            'bg_chloride', 'bg_calcium', 'bg_temperature', 'bg_potassium', 'bg_sodium', 'bg_lactate',
            'bg_glucose', 'bg_tidalvolume',
            #'bg_intubated', 'bg_ventilationrate', 'bg_ventilator',
            'bg_peep', 'bg_o2flow', 'bg_requiredo2',
            # begin lab values
            'aniongap', 'albumin', 'bands', 'bicarbonate', 'bilirubin', 'creatinine',
            'chloride', 'glucose', 'hematocrit', 'hemoglobin', 'lactate', 'platelet',
            'potassium', 'ptt', 'inr', 'pt', 'sodium', 'bun', 'wbc']

    var_last_early = var_first_early
    # fourth set of variables
    # we have special rules for these...
    var_sum = ['urineoutput']

    return var_min, var_max, var_first, var_last, var_sum, var_first_early, var_last_early


def vars_of_interest_streaming():
    # define the covariates to be used in the model
    # these covariates are available in the hospital stream at the moment
    # biggest exclusion compared to the above is no GCS.
    var_min = ['heartrate', 'systolicbp', 'diastolicbp', 'meanbp',
               'resprate', 'spo2'] # , 'temp', 'glucosecharted'
    var_max = var_min
    #var_max.extend(['rrt','vasopressor','vent'])
    #var_min.append('gcs')


    # we extract the first/last value for these covariates
    var_first = ['heartrate', 'systolicbp', 'diastolicbp', 'meanbp',
               'resprate', 'spo2'] # , 'temp', 'glucosecharted'

    var_last = var_first
    #var_last.extend(['gcsmotor','gcsverbal','gcseyes','endotrachflag','gcs'])

    var_first_early = ['bg_so2', 'bg_po2', 'bg_pco2', #'bg_fio2_chartevents', 'bg_aado2_calc',
            'bg_fio2', 'bg_aado2', 'bg_pao2fio2', 'bg_ph', 'bg_baseexcess', 'bg_bicarbonate',
            'bg_totalco2', 'bg_hematocrit', 'bg_hemoglobin', 'bg_carboxyhemoglobin', 'bg_methemoglobin',
            'bg_chloride', 'bg_calcium', 'bg_temperature', 'bg_potassium', 'bg_sodium', 'bg_lactate',
            'bg_glucose', 'bg_tidalvolume',
            # 'bg_intubated', 'bg_ventilationrate', 'bg_ventilator', # these vars are usually NaN
            'bg_peep', 'bg_o2flow', 'bg_requiredo2',
            # begin lab values
            'aniongap', 'albumin', 'bands', 'bicarbonate', 'bilirubin', 'creatinine',
            'chloride', 'glucose', 'hematocrit', 'hemoglobin', 'lactate', 'platelet',
            'potassium', 'ptt', 'inr', 'pt', 'sodium', 'bun', 'wbc']

    var_last_early = var_first_early
    # fourth set of variables
    # we have special rules for these...
    var_sum = None #['urineoutput']

    return var_min, var_max, var_first, var_last, var_sum, var_first_early, var_last_early

def gen_random_offset(df_static, T=4, death_fix=True, T_before_death=None):
    # df_static - dataframe with intime, outtime
    # T - offset time, we ensure we are at least T hours before death
    # T_before_death - if this is not None, we *fix* the offset to be T_before_death hours
    #       this is useful for evaluating how well the algorithm discriminates at T hours
    # death_fix - ensures that outtime is the minimum of (outtime, deathtime)
    tau = np.random.rand(df_static.shape[0])
    df_static['endtime'] = df_static['outtime']

    # ensure that, if the patient died before ICU discharge, we use the earlier time
    if death_fix == True:
        idxFix = (~df_static['deathtime'].isnull()) & (df_static['deathtime'] < df_static['outtime'])
        df_static.loc[idxFix, 'endtime'] = df_static.loc[idxFix, 'deathtime']

    df_static['starttime'] = tau*((df_static['endtime'] - np.timedelta64(T,'h') - df_static['intime']) / np.timedelta64(1,'m'))

    # 10 ICU stays with null outtime - ??? should not be in the DB
    df_static['starttime'].fillna(0,inplace=True)

    # if the stay is shorter than 4 hours, the interval can be negative
    # in this case, we set the interval to 0
    df_static.loc[df_static['starttime'] <  0, 'starttime'] = 0

    # truncate to the minutes df
    df_static['starttime'] = np.floor(df_static['starttime']).astype(int)

    # now update starttime to be equal to (outtime - T) if they died
    if T_before_death is not None:
        df_static.loc[df_static['hospital_expire_flag'] == 1, 'starttime'] = \
        np.floor(\
                 (df_static.loc[df_static['hospital_expire_flag'] == 1, 'endtime'] \
                 - df_static.loc[df_static['hospital_expire_flag'] == 1, 'intime'] \
                 - np.timedelta64(T_before_death,'h')) / np.timedelta64(1,'m')
                 )

    # create a dictionary of starttimes
    start_dict = df_static[['icustay_id','starttime']].set_index('icustay_id').to_dict()['starttime']

    return start_dict

# define the functions we will apply across the data
def first_nonan(x):
    x = x[~x.isnull()]
    if len(x) == 0:
        return np.nan
    else:
        return x.iloc[0]

def last_nonan(x):
    x = x[~x.isnull()]
    if len(x) == 0:
        return np.nan
    else:
        return x.iloc[-1]

def min_nonan(x):
    x = x[~x.isnull()]
    if len(x) == 0:
        return np.nan
    else:
        return np.min(x)

def max_nonan(x):
    x = x[~x.isnull()]
    if len(x) == 0:
        return np.nan
    else:
        return np.max(x)

def sum_nonan(x):
    x = x[~x.isnull()]
    if len(x) == 0:
        return np.nan
    else:
        return np.sum(x)

#TODO: refactor so everything is in seconds

# generate the X data - assume we have given a *single patient's* dataframe
def extract_feature(df, fcnToApply, start=0, offset=24*60*60):
    # df should be indexed by "timeelapsed" - fractional minutes since ICU admission

    # find the nearest start/end time
    # ERROR: the below line doesn't work when start // start+offset aren't in the index.. though I'm not sure why.
    #idx = df.index.slice_indexer(start,start+offset,1)
    # instead, we use searchsorted
    idxStart = df.index.searchsorted(start)
    idxEnd = df.index.searchsorted(start+offset)

    idx = slice(idxStart,idxEnd,1)
    # hack out our dataframe of interest and apply the function
    return df.iloc[idx].apply(fcnToApply)

    # below would reshape to be a single row
    # initialize dataframe - one row with this data
    #return pd.DataFrame(data=np.reshape(X_tmp.values,[1,X_tmp.values.shape[0]]),
    #                    dtype=float, columns=X_tmp.index.values)

def extract_feature_sp(df, start=0, offset=4*60*60):
    df = df.sort_index()

    var_min, var_max, var_first, var_last, var_sum, var_first_early, var_last_early = vars_of_interest()

    X_first = extract_feature(df.loc[:, var_first], first_nonan, start=start, offset=offset)
    X_last = extract_feature(df.loc[:, var_last], last_nonan, start=start, offset=offset)
    X_min = extract_feature(df.loc[:, var_min], min_nonan, start=start, offset=offset)
    X_max = extract_feature(df.loc[:, var_max], max_nonan, start=start, offset=offset)

    # since labs/UO are infrequently sampled, we give them an extra 24 hours for data extraction
    t_add = 24*60*60
    X_first_early = extract_feature(df.loc[:, var_first_early], first_nonan, start=start*60-t_add, offset=offset+t_add)
    X_last_early = extract_feature(df.loc[:, var_last_early], last_nonan, start=start*60-t_add, offset=offset+t_add)
    X_sum = extract_feature(df.loc[:, var_sum], sum_nonan, start=start-t_add, offset=offset+t_add)

    return X_first, X_last, X_first_early, X_last_early, X_min, X_max, X_sum


def extract_feature_ap(df, start_dict, offset=4*60*60):
    # loop across the dataframe - pull data for all patients, then stack together in a new dataframe

    # initialize the dataframes for the data

    var_min, var_max, var_first, var_last, var_sum, var_first_early, var_last_early = vars_of_interest()

    df_first = pd.DataFrame(data=None, dtype=float, columns=var_first)
    df_last  = pd.DataFrame(data=None, dtype=float, columns=var_last)
    df_min = pd.DataFrame(data=None, dtype=float, columns=var_min)
    df_max = pd.DataFrame(data=None, dtype=float, columns=var_max)
    df_sum = pd.DataFrame(data=None, dtype=float, columns=var_sum)
    df_first_early  = pd.DataFrame(data=None, dtype=float, columns=var_first_early)
    df_last_early   = pd.DataFrame(data=None, dtype=float, columns=var_last_early)

    for iid in df.index.levels[0]:
        X_first, X_last, X_first_early, X_last_early, X_min, X_max, X_sum = \
        extract_feature_sp(df.loc[iid, :], start=start_dict[iid], offset=offset)

        # set the name of the series - when we append it to the dataframe, this becomes the index value
        X_first.name = iid
        X_last.name = iid
        X_first_early.name = iid
        X_last_early.name = iid
        X_min.name = iid
        X_max.name = iid
        X_sum.name = iid

        df_first = df_first.append(X_first)
        df_last = df_last.append(X_last)
        df_first_early = df_first_early.append(X_first_early)
        df_last_early = df_last_early.append(X_last_early)
        df_min = df_min.append(X_min)
        df_max = df_max.append(X_max)
        df_sum = df_sum.append(X_sum)

    # update the column names
    df_first.columns = [x + '_first' for x in df_first.columns]
    df_last.columns = [x + '_last' for x in df_last.columns]
    df_first_early.columns = [x + '_first' for x in df_first_early.columns]
    df_last_early.columns = [x + '_last' for x in df_last_early.columns]
    df_min.columns = [x + '_min' for x in df_min.columns]
    df_max.columns = [x + '_max' for x in df_max.columns]
    df_sum.columns = [x + '_sum' for x in df_sum.columns]

    # now combine all the arrays together
    df_data = pd.concat([df_first, df_first_early, df_last, df_last_early, df_min, df_max, df_sum], axis=1)
    df_data

    return df_data


def extract_feature_across_sp(df, T=4*60*60):
    """Given a dataframe for a single patient, this extracts features at *every* row

    The time elapsed (measured in minutes) should be the only index
    """
    df = df.sort_index()

    var_min, var_max, var_first, var_last, var_sum, var_first_early, var_last_early = vars_of_interest()

    df_first = pd.DataFrame(data=None, dtype=float, columns=var_first)
    df_last  = pd.DataFrame(data=None, dtype=float, columns=var_last)
    df_min = pd.DataFrame(data=None, dtype=float, columns=var_min)
    df_max = pd.DataFrame(data=None, dtype=float, columns=var_max)
    df_sum = pd.DataFrame(data=None, dtype=float, columns=var_sum)
    df_first_early  = pd.DataFrame(data=None, dtype=float, columns=var_first_early)
    df_last_early   = pd.DataFrame(data=None, dtype=float, columns=var_last_early)

    for t in df.index:
        X_first, X_last, X_first_early, X_last_early, X_min, X_max, X_sum = \
        extract_feature_sp(df, start=t, offset=T)

        # set the name of the series - when we append it to the dataframe, this becomes the index value
        X_first.name = t
        X_last.name = t
        X_first_early.name = t
        X_last_early.name = t
        X_min.name = t
        X_max.name = t
        X_sum.name = t

        df_first = df_first.append(X_first)
        df_last = df_last.append(X_last)
        df_first_early = df_first_early.append(X_first_early)
        df_last_early = df_last_early.append(X_last_early)
        df_min = df_min.append(X_min)
        df_max = df_max.append(X_max)
        df_sum = df_sum.append(X_sum)

    # update the column names
    df_first.columns = [x + '_first' for x in df_first.columns]
    df_last.columns = [x + '_last' for x in df_last.columns]
    df_first_early.columns = [x + '_first' for x in df_first_early.columns]
    df_last_early.columns = [x + '_last' for x in df_last_early.columns]
    df_min.columns = [x + '_min' for x in df_min.columns]
    df_max.columns = [x + '_max' for x in df_max.columns]
    df_sum.columns = [x + '_sum' for x in df_sum.columns]

    # now combine all the arrays together
    df_data = pd.concat([df_first, df_first_early, df_last, df_last_early, df_min, df_max, df_sum], axis=1)
    df_data

    return df_data


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



def plot_xgb_importance_fmap(xgb_model, X_header=None, ax=None, height=0.2,
                    xlim=None, ylim=None, title='Feature importance',
                    xlabel='F score', ylabel='Features',
                    importance_type='weight',
                    grid=True, **kwargs):

    fmap = xgb_model.booster().get_score(importance_type=importance_type)

    if X_header is not None:
        feat_map = {}
        for i in range(len(X_header)):
            feat_map['f' + str(i)] = X_header[i]

        importance = {}
        for i in fmap:
            importance[ feat_map[i] ] = fmap[i]
    else:
        importance = fmap

    tuples = [(k, importance[k]) for k in importance]
    tuples = sorted(tuples, key=lambda x: x[1])
    labels, values = zip(*tuples)

    if ax is None:
        _, ax = plt.subplots(1, 1)

    ylocs = np.arange(len(values))
    ax.barh(ylocs, values, align='center', height=height, **kwargs)

    for x, y in zip(values, ylocs):
        ax.text(x + 1, y, x, va='center')

    ax.set_yticks(ylocs)
    ax.set_yticklabels(labels)

    if xlim is not None:
        if not isinstance(xlim, tuple) or len(xlim) != 2:
            raise ValueError('xlim must be a tuple of 2 elements')
    else:
        xlim = (0, max(values) * 1.1)
    ax.set_xlim(xlim)

    if ylim is not None:
        if not isinstance(ylim, tuple) or len(ylim) != 2:
            raise ValueError('ylim must be a tuple of 2 elements')
    else:
        ylim = (-1, len(importance))
    ax.set_ylim(ylim)

    if title is not None:
        ax.set_title(title)
    if xlabel is not None:
        ax.set_xlabel(xlabel)
    if ylabel is not None:
        ax.set_ylabel(ylabel)
    ax.grid(grid)
    return ax


def load_design_matrix(co, df_additional_data=None, data_ext='', path=None, diedWithin=None):
    # this function loads in the data from csv
    # co is a dataframe with:
    #    - patients to include (all the icustay_ids in the index)
    #    - the outcome (first and only column)

    if path is None:
        path = ''

    if data_ext != '' and data_ext[0] != '_':
        data_ext = '_' + data_ext

    df_offset = pd.read_csv(path + 'icustays_offset' + data_ext + '.csv')
    df_offset['intime'] = pd.to_datetime(df_offset['intime'])
    df_offset['outtime'] = pd.to_datetime(df_offset['outtime'])
    df_offset['deathtime'] = pd.to_datetime(df_offset['deathtime'])
    df_offset['icustay_id'] = df_offset['icustay_id'].astype(int)
    df_offset = df_offset.loc[:,['icustay_id','intime','outtime','starttime','deathtime']]
    df_offset.set_index('icustay_id',inplace=True)

    # load in the design matrix
    df_design = pd.read_csv(path + 'design_matrix' + data_ext + '.csv')
    df_design['icustay_id'] = df_design['icustay_id'].astype(int)
    df_design.set_index('icustay_id',inplace=True)


    # join these dfs together, add in the static vars
    df = co.merge(df_design, how='left', left_index=True, right_index=True)
    if df_additional_data is not None:
        df = df.merge(df_additional_data,how='left', left_index=True, right_index=True)

    # change y to be "died within 24 hours"
    if diedWithin is not None:
        df = df.merge(df_offset[['intime','deathtime','starttime']],
        how='left', left_index=True, right_index=True)

        df['hospital_expire_flag'] = np.zeros(df.shape[0],dtype=int)
        idxUpdate = ~df['deathtime'].isnull()
        df.loc[idxUpdate,'hospital_expire_flag'] = (df.loc[idxUpdate,'deathtime'] <
                                                        (df.loc[idxUpdate,'intime']
                                                         + pd.to_timedelta(df.loc[idxUpdate,'starttime'], 'm')
                                                         + np.timedelta64(diedWithin, 'h')))

        # drop the columns temporarily added to redefine the outcome
        df.drop('intime',axis=1,inplace=True)
        df.drop('starttime',axis=1,inplace=True)
        df.drop('deathtime',axis=1,inplace=True)

    # HACK: drop some variables here we are not interested in
    # they should be removed from vars_of_interest and data extraction re-run
    vars_to_delete = ['bg_intubated_first', 'bg_ventilationrate_first', 'bg_ventilator_first',
    'bg_intubated_last', 'bg_ventilationrate_last', 'bg_ventilator_last',
    'rrt_min', 'vasopressor_min', 'vent_min',
    'rrt_max', 'vasopressor_max', 'vent_max']
    for v in vars_to_delete:
        if v in df.columns:
            df.drop(v, axis=1, inplace=True)

    # move from a data frame into a numpy array
    X = df.values.astype(float)
    y = X[:,0]

    icustay_id = df.index.values

    # delete first column: the outcome
    X = np.delete(X,0,axis=1)

    # get a header row
    X_header = [xval for x, xval in enumerate(df.columns) if x > 0]

    return X, y, X_header
