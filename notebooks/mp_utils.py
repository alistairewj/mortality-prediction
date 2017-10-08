# Import libraries
import numpy as np
import pandas as pd
import psycopg2
import sys
import datetime as dt
from sklearn import metrics
import matplotlib.pyplot as plt

# default colours for prettier plots
col = [[0.9047, 0.1918, 0.1988],
    [0.2941, 0.5447, 0.7494],
    [0.3718, 0.7176, 0.3612],
    [1.0000, 0.5482, 0.1000],
    [0.4550, 0.4946, 0.4722],
    [0.6859, 0.4035, 0.2412],
    [0.9718, 0.5553, 0.7741],
    [0.5313, 0.3359, 0.6523]];
marker = ['v','o','d','^','s','o','+']
ls = ['-','-','-','-','-','s','--','--']

def generate_times(df, T=None, T_to_death=None, seed=None, censor=False):
    # generate a dictionary based off of the analysis type desired
    # creates "windowtime" - the time at the end of the window

    # df needs to have the following fields:
    #   icustay_id (not as an index)
    #   dischtime_hours
    #   deathtime_hours
    #   censortime_hours (if censoring with censor=True)
    # these times are relative to ICU intime ("_hours" means hours after ICU admit)
    if seed is None:
        print('Using default seed 111.')
        seed=111

    # create endtime: this is the last allowable time for our window
    df['endtime'] = df['dischtime_hours']

    # if they die before discharge, set the end time to the time of death
    idx = (~df['deathtime_hours'].isnull()) & (df['deathtime_hours']<df['dischtime_hours'])
    df.loc[idx,'endtime'] = df.loc[idx,'deathtime_hours']

    # optionally censor the data
    # this involves updating the endtime to an even earlier time, if present
    # e.g. the first time a patient was made DNR
    if censor:
        idx = (~df['censortime_hours'].isnull()) & (df['censortime_hours']<df['endtime'])
        df.loc[idx,'endtime'] = df.loc[idx,'censortime_hours']

    # now generate the end of the window
    # this is X hours
    np.random.seed(seed)
    tau = np.random.rand(df.shape[0])
    # T adds a bit of fuzziness to prevent information leakage
    if T is not None:
        # extract window at least T hours before discharge/death
        df['windowtime'] = np.floor(tau*(df['endtime']-T))
        # if the stay is shorter than T hours, the interval can be negative
        # in this case, we set the interval to 0
        # usually, this will mean we only have lab data
        df.loc[df['windowtime']<0, 'windowtime'] = 0
    else:
        df['windowtime'] = np.floor(tau*(df['endtime']))


    if T_to_death is not None:
        # fix the time for those who die to be T_to_death hours from death
        # first, isolate patients where they were in the ICU T hours before death
        idxInICU = (df['deathtime_hours'] - df['dischtime_hours'])<=T_to_death
        # for these patients, set the time to be T_to_death hours
        df.loc[idxInICU, 'windowtime'] = df.loc[idxInICU,'deathtime_hours'] - T_to_death

    windowtime_dict = df.set_index('icustay_id')['windowtime'].to_dict()
    return windowtime_dict


def generate_times_before_death(df, T=None, T_to_death=None, seed=None):
    # generate a dictionary based off of the analysis type desired
    # creates "windowtime" - the time at the end of the window

    # df needs to have the following fields:
    #   icustay_id (not as an index)
    #   dischtime_hours
    #   deathtime_hours

    if seed is None:
        print('Using default seed 111.')
        seed=111
    df['endtime'] = df['dischtime_hours']
    idx = (~df['deathtime_hours'].isnull()) & (df['deathtime_hours']<df['dischtime_hours'])
    df.loc[idx,'endtime'] = df.loc[idx,'deathtime_hours']


    np.random.seed(seed)

    # df is centered on intime (as t=0)
    # we need to ensure a random time is at least T hours from death/discharge
    tau = np.random.rand(df.shape[0])

    if T is not None:
        # extract window at least T hours before discharge/death
        df['windowtime'] = np.floor(tau*(df['endtime']-T))
        # if the stay is shorter than T hours, the interval can be negative
        # in this case, we set the interval to 0
        df.loc[df['windowtime']<0, 'windowtime'] = 0
    else:
        df['windowtime'] = np.floor(tau*(df['endtime']))

    if T_to_death is not None:
        # fix the time for those who die to be T_to_death hours from death
        # first, isolate patients where they were in the ICU T hours before death
        idxInICU = (df['deathtime_hours'] - df['dischtime_hours'])<=T_to_death
        # for these patients, set the time to be T_to_death hours
        df.loc[idxInICU, 'windowtime'] = df.loc[idxInICU,'deathtime_hours'] - T_to_death

    windowtime_dict = df.set_index('icustay_id')['windowtime'].to_dict()
    return windowtime_dict

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
    var_min = ['heartrate', 'sysbp', 'diasbp', 'meanbp',
                'resprate', 'tempc', 'spo2']
    var_max = var_min
    var_min.append('gcs')
    #var_max.extend(['rrt','vasopressor','vent'])

    # we extract the first/last value for these covariates
    var_first = ['heartrate', 'sysbp', 'diasbp', 'meanbp',
                'resprate', 'tempc', 'spo2']

    var_last = var_first
    var_last.extend(['gcsmotor','gcsverbal','gcseyes','endotrachflag','gcs'])

    var_first_early = ['bg_po2', 'bg_pco2', #'bg_so2'
            #'bg_fio2_chartevents', 'bg_aado2_calc',
            #'bg_fio2', 'bg_aado2',
            'bg_pao2fio2ratio', 'bg_ph', 'bg_baseexcess', #'bg_bicarbonate',
            'bg_totalco2', #'bg_hematocrit', 'bg_hemoglobin',
            'bg_carboxyhemoglobin', 'bg_methemoglobin',
            #'bg_chloride', 'bg_calcium', 'bg_temperature',
            #'bg_potassium', 'bg_sodium', 'bg_lactate',
            #'bg_glucose',
            # 'bg_tidalvolume', 'bg_intubated', 'bg_ventilationrate', 'bg_ventilator',
            # 'bg_peep', 'bg_o2flow', 'bg_requiredo2',
            # begin lab values
            'aniongap', 'albumin', 'bands', 'bicarbonate', 'bilirubin', 'creatinine',
            'chloride', 'glucose', 'hematocrit', 'hemoglobin', 'lactate', 'platelet',
            'potassium', 'ptt', 'inr', 'sodium', 'bun', 'wbc']

    var_last_early = var_first_early
    # fourth set of variables
    # we have special rules for these...
    var_sum = ['urineoutput']

    var_static = [u'is_male', u'emergency_admission', u'age',
               # services
               u'service_any_noncard_surg',
               u'service_any_card_surg',
               u'service_cmed',
               u'service_traum',
               u'service_nmed',
               # ethnicities
               u'race_black',u'race_hispanic',u'race_asian',u'race_other',
               # demographics
               u'height', u'weight', u'bmi']

    return var_min, var_max, var_first, var_last, var_sum, var_first_early, var_last_early, var_static


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
            'bg_glucose',
            # 'bg_intubated', 'bg_ventilationrate', 'bg_ventilator', # these vars are usually NaN
            # 'bg_tidalvolume', 'bg_peep', 'bg_o2flow', 'bg_requiredo2',
            # begin lab values
            'aniongap', 'albumin', 'bands', 'bicarbonate', 'bilirubin', 'creatinine',
            'chloride', 'glucose', 'hematocrit', 'hemoglobin', 'lactate', 'platelet',
            'potassium', 'ptt', 'inr', 'pt', 'sodium', 'bun', 'wbc']

    var_last_early = var_first_early
    # fourth set of variables
    # we have special rules for these...
    var_sum = None #['urineoutput']

    return var_min, var_max, var_first, var_last, var_sum, var_first_early, var_last_early


def get_design_matrix(df, time_dict, W=8, W_extra=24):
    # W_extra is the number of extra hours to look backward for labs
    # e.g. if W_extra=24 we look back an extra 24 hours for lab values

    # timing info for icustay_id < 200100:
    #   5 loops, best of 3: 877 ms per loop

    # timing info for all icustay_id:
    #   5 loops, best of 3: 1.48 s per loop

    # get the hardcoded variable names
    var_min, var_max, var_first, var_last, var_sum, var_first_early, var_last_early, var_static = vars_of_interest()

    tmp = np.asarray(time_dict.items()).astype(int)
    N = tmp.shape[0]

    M = W+W_extra
    # create a vector of [0,...,M] to represent the hours we need to subtract for each icustay_id
    hr = np.linspace(0,M,M+1,dtype=int)
    hr = np.reshape(hr,[1,M+1])
    hr = np.tile(hr,[N,1])
    hr = np.reshape(hr, [N*(M+1),], order='F')

    # duplicate tmp to M+1, as we will be creating T+1 rows for each icustay_id
    tmp = np.tile(tmp,[M+1,1])

    tmp_early_flag = np.copy(tmp[:,1])

    # adding hr to tmp[:,1] gives us what we want: integers in the range [Tn-T, Tn]
    tmp = np.column_stack([tmp[:,0], tmp[:,1]-hr, hr>W])

    # create dataframe with tmp
    df_time = pd.DataFrame(data=tmp, index=None, columns=['icustay_id','hr','early_flag'])
    df_time.sort_values(['icustay_id','hr'],inplace=True)

    # merge df_time with df to filter down to a subset of rows
    df = df.merge(df_time, left_on=['icustay_id','hr'], right_on=['icustay_id','hr'],how='inner')

    # apply functions to groups of vars
    df_first_early  = df.groupby('icustay_id')[var_first_early].first()
    df_last_early   = df.groupby('icustay_id')[var_last_early].last()


    # slice down df_time by removing early times
    # isolate only have data from [t - W, t - W + 1, ..., t]
    df = df.loc[df['early_flag']==0,:]

    df_first = df.groupby('icustay_id')[var_first].first()
    df_last  = df.groupby('icustay_id')[var_last].last()
    df_min = df.groupby('icustay_id')[var_min].min()
    df_max = df.groupby('icustay_id')[var_max].max()
    df_sum = df.groupby('icustay_id')[var_sum].sum()

    # update the column names
    df_first.columns = [x + '_first' for x in df_first.columns]
    df_last.columns = [x + '_last' for x in df_last.columns]
    df_first_early.columns = [x + '_first_early' for x in df_first_early.columns]
    df_last_early.columns = [x + '_last_early' for x in df_last_early.columns]
    df_min.columns = [x + '_min' for x in df_min.columns]
    df_max.columns = [x + '_max' for x in df_max.columns]
    df_sum.columns = [x + '_sum' for x in df_sum.columns]

    # now combine all the arrays together
    df_data = pd.concat([df_first, df_first_early, df_last, df_last_early, df_min, df_max, df_sum], axis=1)

    return df_data

# this function is used to print out data for a single pt
# mainly used for debugging weird inconsistencies in data extraction
# e.g. "wait why does this icustay_id not have heart rate?"
def debug_for_iid(df, time_dict, iid, T=8, W_extra=24):
    #tmp = np.asarray(time_dict.items()).astype(int)
    tmp = np.asarray([iid, time_dict[iid]]).astype(int)
    tmp = np.reshape(tmp,[1,2])
    N = tmp.shape[0]

    M = W+W_extra
    # create a vector of [0,...,M] to represent the hours we need to subtract for each icustay_id
    hr = np.linspace(0,M,M+1,dtype=int)
    hr = np.reshape(hr,[1,M+1])
    hr = np.tile(hr,[N,1])
    hr = np.reshape(hr, [N*(M+1),], order='F')

    # duplicate tmp to M+1, as we will be creating T+1 rows for each icustay_id
    tmp = np.tile(tmp,[M+1,1])

    tmp_early_flag = np.copy(tmp[:,1])

    # adding hr to tmp[:,1] gives us what we want: integers in the range [Tn-T, Tn]
    tmp = np.column_stack([tmp[:,0], tmp[:,1]-hr, hr>T])

    # create dataframe with tmp
    df_time = pd.DataFrame(data=tmp, index=None, columns=['icustay_id','hr','early_flag'])
    df_time.sort_values(['icustay_id','hr'],inplace=True)

    # display the data for this icustay_id
    print('\n\n ALL DATA FOR THIS ICUSTAY_ID \n\n')
    display(HTML(df.loc[df['icustay_id']==iid,:].to_html().replace('NaN','')))

    # display the times selected for this icustay_id
    print('\n\n TIMES FOR THIS ICUSTAY_ID \n\n')
    display(HTML(df_time.loc[df_time['icustay_id']==iid].to_html().replace('NaN','')))

    # merge df_time with df to filter down to a subset of rows
    df = df.loc[df['icustay_id']==iid,:].merge(df_time, left_on=['icustay_id','hr'], right_on=['icustay_id','hr'],how='inner')
    df = df.loc[df['early_flag']==0,:]
    display(HTML(df.to_html().replace('NaN','')))

    print('\n\nFIRST\n\n')
    display(HTML(df_tmp.groupby('icustay_id')[var_first].first().to_html().replace('NaN','')))
    print('\n\nLAST\n\n')
    display(HTML(df_tmp.groupby('icustay_id')[var_first].last().to_html().replace('NaN','')))


def collapse_data(data):
    # this collapses a dictionary of dataframes into a single dataframe
    # joins them together on icustay_id and charttime
    files = data.keys()
    initFlag = False # tells the function to create a new dataframe

    # dictionary mapping table names to column name of interest
    colNameMap = {'vent': 'vent',
                  'vasopressor': 'vasopressor',
                  'rrt_range': 'rrt'}
    rangeTbl = ['vent','vasopressor','rrt_range']

    # merge all data from above dictionary into a single data frame
    for i, f in enumerate(files):
        df_tmp = data[f]

        if 'subject_id' in df_tmp.columns:
            df_tmp.drop('subject_id',axis=1,inplace=True)
        if 'hadm_id' in df_tmp.columns:
            df_tmp.drop('hadm_id',axis=1,inplace=True)
        if 'storetime' in df_tmp.columns:
            df_tmp.drop('storetime',axis=1,inplace=True)

        print('{:20s}... finished.'.format(f))
        if f in rangeTbl:
            continue # these are rangesignal tables.. need to be handled separately

        if initFlag == False:
            df = df_tmp.copy()
            initFlag = True
            continue
        else:
            df = df.merge(df_tmp,
                            on=['icustay_id','charttime_elapsed'],
                            how='outer')

        return df


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

def plot_vitals(df, iid, df_death=None, df_censor=None):
    plt.figure(figsize=[14,10])
    idx = df['icustay_id']==iid
    plt.plot( df.loc[idx, 'hr'], df.loc[idx, 'heartrate'], 'ro-', label='Heart rate' )
    plt.plot( df.loc[idx, 'hr'], df.loc[idx, 'sysbp'], 'b^', label='Systolic BP', alpha=0.5 )
    plt.plot( df.loc[idx, 'hr'], df.loc[idx, 'diasbp'], 'bv', label='Diastolic BP', alpha=0.5 )
    plt.plot( df.loc[idx, 'hr'], df.loc[idx, 'meanbp'], 'bd', label='Mean BP', alpha=0.8 )
    plt.plot( df.loc[idx, 'hr'], df.loc[idx, 'resprate'], 'go', label='Respiratory rate', alpha=0.5 )

    if df_death is not None:
        # add in discharge/death time
        idx = df_death['icustay_id']==iid
        plt.plot( np.ones([2,])*df_death.loc[idx, 'dischtime_hours'].values, [0,200], 'k--', linewidth=2, label='Time of discharge' )
        if df_death.loc[idx,'deathtime_hours'] is not np.nan:
            plt.plot( np.ones([2,])*df_death.loc[idx, 'deathtime_hours'].values, [0,200], 'k-', linewidth=2, label='Time of death' )
            plt.plot( np.ones([2,])*df_death.loc[idx, 'deathtime_hours'].values-24, [0,200], 'k:', linewidth=2, label='24 hr before death' )
            plt.title('Died in hospital',fontsize=20)
        plt.xlim([-1, df_death.loc[idx, 'dischtime_hours'].values+6])


    if df_censor is not None:
        idx = df_censor['icustay_id']==iid
        if np.any(idx):
            plt.plot( np.ones([2,])*df_censor.loc[idx, 'censortime_hours'].values, [0,200],
            'm--', alpha=0.8, linewidth=3, label='DNR' )


    plt.xlabel('Hours since ICU admission', fontsize=20)
    plt.ylabel('Vital sign value', fontsize=20)
    plt.grid()
    plt.legend(loc='best')
    plt.show()

def plot_model_results(results, pretty_labels=None):
    if pretty_labels is None:
        pretty_labels = {'xgb': 'GB', 'rf': 'RF', 'logreg': 'LR', 'lasso': 'LASSO'}

    # make sure pretty_labels has all model names as a key
    for x in results.keys():
        if x not in pretty_labels:
            pretty_labels[x] = x

    plt.figure(figsize=[12,8])
    for m, mdl in enumerate(results):
        curr_score = results[mdl]
        plt.plot(m*np.ones(len(curr_score)), curr_score,
                marker=marker[m], color=col[m],
                markersize=10, linewidth=2, linestyle=':',
                label=pretty_labels[mdl])

    plt.ylabel('AUROC',fontsize=18)
    plt.xlim([-1,m+1])
    plt.ylim([0.7,1.0])
    plt.grid()
    plt.gca().set_xticks(np.linspace(0,m,m+1))

    plt.gca().set_xticklabels([pretty_labels[x] for x in results.keys()])
    for tick in plt.gca().xaxis.get_major_ticks():
        tick.label.set_fontsize(20)

    for tick in plt.gca().yaxis.get_major_ticks():
        tick.label.set_fontsize(16)

    #plt.legend(loc='lower right',fontsize=18)
    plt.show()

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

    # change y to be "died within X seconds", where X is specified by the user
    if diedWithin is not None:
        df = df.merge(df_offset[['intime','deathtime','starttime']],
        how='left', left_index=True, right_index=True)

        df['hospital_expire_flag'] = np.zeros(df.shape[0],dtype=int)
        idxUpdate = ~df['deathtime'].isnull()
        df.loc[idxUpdate,'hospital_expire_flag'] = (df.loc[idxUpdate,'deathtime'] <
                                                        (df.loc[idxUpdate,'intime']
                                                         + pd.to_timedelta(df.loc[idxUpdate,'starttime'], 's')
                                                         + np.timedelta64(diedWithin, 's')))

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

def get_predictions(df, df_static,  mdl, iid):
    df = df.loc[df['icustay_id']==iid,:]
    tm = df['hr'].values
    prob = list()
    var_min, var_max, var_first, var_last, var_sum, var_first_early, var_last_early, var_static = vars_of_interest()

    for t in tm:
        time_dict = {iid: t}
        X = get_design_matrix(df, time_dict, W=4, W_extra=24)

        # first, the data from static vars from df_static
        X = X.merge(df_static.set_index('icustay_id')[var_static], how='left', left_index=True, right_index=True)

        # convert to numpy data
        X = X.values

        curr_prob = mdl.predict_proba(X)
        prob.append(curr_prob[0,1])

    return tm, prob


def get_data_at_time(df, df_static, iid, hour=0):
    df = df.loc[df['icustay_id']==iid,:]
    tm = df['hr'].values
    var_min, var_max, var_first, var_last, var_sum, var_first_early, var_last_early, var_static = vars_of_interest()

    idx = [i for i, tval in enumerate(tm) if tval==hour]
    if len(idx)==0:
        idx = [i for i,j in enumerate(tm) if i<hour]
        if len(idx)==0:
            idx = 0
        else:
            idx = idx[-1]
        print('Hour not found! Using closest previous value: {}.'.format(tm[idx]))
    else:
        idx=idx[0]

    t=tm[idx]
    time_dict = {iid: t}
    X = get_design_matrix(df, time_dict, W=4, W_extra=24)

    # first, the data from static vars from df_static
    X = X.merge(df_static.set_index('icustay_id')[var_static], how='left', left_index=True, right_index=True)

    # convert to numpy data
    X = X.values

    return X
