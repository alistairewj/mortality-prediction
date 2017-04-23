-- this query counts the number of observations for patients
-- it counts only during the patient's ICU stay

drop table if exists dm_obs_count cascade;
create table dm_obs_count as
with ie_cv as
(
  select icustay_id, count(*) as iv
  -- only "MedEvents" (As was done in Hug 2009)
  , count(rate) as iv_rate
  -- saps-I var  (urine output)
  , SUM (CASE WHEN ITEMID IN
      (
        651, 715, 55, 56, 57, 61, 65, 69, 85, 94, 96,
        288, 405, 428, 473,
        2042, 2068, 2111, 2119, 2130, 1922, 2810, 2859,
        3053, 3462, 3519, 3175, 2366, 2463, 2507, 2510,
        2592, 2676, 3966, 3987, 4132, 4253, 5927
      ) THEN 1 ELSE 0 END) AS urineoutput

  from inputevents_cv
  group by icustay_id
)
, ie_mv as
(
  select icustay_id, count(*) as iv
  , count(rate) as iv_rate
  -- my replication of SAPS-I var in metavision (urine output)
  , SUM(CASE WHEN ITEMID IN
      (
      226559, -- "Foley"
      226560, -- "Void"
      226561, -- "Condom Cath"
      226584, -- "Ileoconduit"
      226563, -- "Suprapubic"
      226564, -- "R Nephrostomy"
      226565, -- "L Nephrostomy"
      226567, --	Straight Cath
      226557, -- R Ureteral Stent
      226558, -- L Ureteral Stent
      227488, -- GU Irrigant Volume In
      227489  -- GU Irrigant/Urine Volume Out
      ) THEN 1 ELSE 0 END) AS urineoutput
  from inputevents_mv
  group by icustay_id
)
, labs as
(
  select ie.icustay_id
    , SUM(CASE WHEN itemid = 50868 THEN 1 ELSE 0 END) as ANIONGAP
    , SUM(CASE WHEN itemid = 50862 THEN 1 ELSE 0 END) as ALBUMIN
    , SUM(CASE WHEN itemid = 51144 THEN 1 ELSE 0 END) as BANDS
    , SUM(CASE WHEN itemid = 50882 THEN 1 ELSE 0 END) as BICARBONATE
    , SUM(CASE WHEN itemid = 50885 THEN 1 ELSE 0 END) as BILIRUBIN
    , SUM(CASE WHEN itemid = 50912 THEN 1 ELSE 0 END) as CREATININE
    , SUM(CASE WHEN itemid in (50806,50902) THEN 1 ELSE 0 END) as CHLORIDE
    , SUM(CASE WHEN itemid in (50809,50931) THEN 1 ELSE 0 END) as GLUCOSE
    , SUM(CASE WHEN itemid in (50810,51221) THEN 1 ELSE 0 END) as HEMATOCRIT
    , SUM(CASE WHEN itemid in (50811,51222) THEN 1 ELSE 0 END) as HEMOGLOBIN
    , SUM(CASE WHEN itemid = 50813 THEN 1 ELSE 0 END) as LACTATE
    , SUM(CASE WHEN itemid = 51265 THEN 1 ELSE 0 END) as PLATELET
    , SUM(CASE WHEN itemid in (50822,50971) THEN 1 ELSE 0 END) as POTASSIUM
    , SUM(CASE WHEN itemid = 51275 THEN 1 ELSE 0 END) as PTT
    , SUM(CASE WHEN itemid = 51237 THEN 1 ELSE 0 END) as INR
    , SUM(CASE WHEN itemid = 51274 THEN 1 ELSE 0 END) as PT
    , SUM(CASE WHEN itemid in (50824,50983) THEN 1 ELSE 0 END) as SODIUM
    , SUM(CASE WHEN itemid = 51006 THEN 1 ELSE 0 END) as BUN
    , SUM(CASE WHEN itemid in (51300,51301) THEN 1 ELSE 0 END) as WBC

    -- blood gases
    , SUM(case when itemid = 50821 then 1 else 0 end) as PO2
    , SUM(case when itemid = 50820 then 1 else 0 end) as PH

    --  SAPS-I labs !
    , SUM(CASE WHEN itemid in (
        50810, 51221,        -- HCT
        51300, 51301, -- WBC
        50809, 50931, -- Glucose
        50882, -- HCO3
        50822, 50971, -- Potassium
        50824, 50983,  -- Sodium
        51006         -- BUN
      ) THEN 1 ELSE 0 END) as saps_labs
    -- SOFA labs
    , SUM(CASE WHEN itemid in (
        50885, -- bilirubin
        51265, -- platelets
        50912  -- creatinine
      ) THEN 1 ELSE 0 END) as sofa_labs

  from icustays ie
  left join labevents le
    on ie.hadm_id = le.hadm_id
    and le.charttime between ie.intime and ie.outtime
  group by ie.icustay_id
)
-- charted data
, chart as
(

  select ce.icustay_id
  , SUM(case when itemid in (211,220045) then 1 else 0 end) as HeartRate
  , SUM(case when itemid in (51,442,455,6701,220179,220050) then 1 else 0 end) as SysBP
  , SUM(case when itemid in (8368,8440,8441,8555,220180,220051) then 1 else 0 end) as DiasBP
  , SUM(case when itemid in (456,52,6702,443,220052,220181,225312) then 1 else 0 end) as MeanBP
  , SUM(case when itemid in (615,618,220210,224690) then 1 else 0 end) as RespRate
  , SUM(case when itemid in (223761,678,223762,676) then 1 else 0 end) as Temp
  , SUM(case when itemid in (646,220277) then 1 else 0 end) as SpO2
  , SUM(case when itemid in (807,811,1529,3745,3744,225664,220621,226537) then 1 else 0 end) as Glucose
  , SUM(case when itemid in (184, 454, 723, 223900, 223901, 220739) then 1 else 0 end) as GCS
  -- missing SAPS-I
  , SUM(CASE WHEN itemid IN
    (
      -- vitals/gcs etc
      211,220045,
      676, 677, 678, 679,223761,223762,
      51,455,220179,220050,
      781,225624, -- BUN
      184, 454, 723, 223900, 223901, 220739,
      -- breathing params
      615,618,220210,224690
    ) then 1 else 0 end) as saps_chart

  -- missing SOFA
  , SUM(CASE WHEN itemid IN
    (
      189, 190, 2981, 7570, 3420, 3422, 223835,-- fio2
      490, 779, 220224,-- pao2
      52,456,220052,220181,225312, -- mbp
      184, 454, 723, 223900, 223901, 220739 -- gcs
    ) then 1 else 0 end) as sofa_chart
  from chartevents ce
  group by ce.icustay_id
)
select
  ie.icustay_id
  -- vitals
  , chart.heartrate
  , chart.sysbp
  , chart.diasbp
  , chart.meanbp
  , chart.resprate
  , chart.temp
  , chart.spo2
  , chart.gcs

  -- labs
  , labs.ANIONGAP
  , labs.ALBUMIN
  , labs.BANDS
  , labs.BICARBONATE
  , labs.BILIRUBIN
  , labs.CREATININE
  , labs.CHLORIDE
  , labs.HEMATOCRIT
  , labs.HEMOGLOBIN
  , labs.LACTATE
  , labs.PLATELET
  , labs.POTASSIUM
  , labs.PTT
  , labs.INR
  , labs.PT
  , labs.SODIUM
  , labs.BUN
  , labs.WBC

  -- bgs
  , labs.po2
  , labs.ph

  -- both (may double count some observations)
  , chart.glucose + labs.GLUCOSE as glucose

  -- "any IV recording"
  , coalesce(ie_cv.iv,0) + coalesce(ie_mv.iv,0) as iv
  , coalesce(ie_cv.iv_rate,0) + coalesce(ie_mv.iv_rate,0) as iv_rate

  -- saps
  ,  coalesce(labs.saps_labs,0)
    + coalesce(chart.saps_chart,0)
    + coalesce(ie_cv.urineoutput,0)
    + coalesce(ie_mv.urineoutput,0)
    as saps_vars

  -- sofa
  ,  coalesce(labs.sofa_labs,0)
    + coalesce(chart.sofa_chart,0)
    + coalesce(ie_cv.urineoutput,0)
    + coalesce(ie_mv.urineoutput,0)
    + case when vaso.icustay_id is not null then 1 else 0 end
    as sofa_vars
from icustays ie
left join labs
  on ie.icustay_id = labs.icustay_id
left join ie_cv
on ie.icustay_id = ie_cv.icustay_id
left join ie_mv
  on ie.icustay_id = ie_mv.icustay_id
left join chart
  on ie.icustay_id = chart.icustay_id
-- below used for sofa
left join (select distinct icustay_id from vasopressordurations) vaso
  on ie.icustay_id = vaso.icustay_id
order by ie.icustay_id;
