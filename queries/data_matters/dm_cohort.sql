DROP TABLE IF EXISTS dm_cohort CASCADE;
CREATE TABLE dm_cohort AS

-- tr.rn is the last unit the patient stayed in the hospital
-- we join to tr to check if the last unit the patient stayed in was an icu
with tr as
(
select hadm_id, icustay_id, intime, outtime, curr_careunit
, ROW_NUMBER() over (partition by hadm_id order by intime desc) as rn
from mimiciii.transfers
where outtime is not null
)
, ds as
(
  select distinct hadm_id
  from noteevents
  where category = 'Discharge summary'
)
-- any non-full code status is excluded in hug 2009
, fullcode as
(

  select
      icustay_id
    , max(FullCode) as fullcode
    , max(case when CMO=1 or cmo_notes=1 then 1 else 0 end) as cmo
    , max(DNR) as dnr
    , max(DNI) as dni
    , max(DNCPR) as dncpr
  from mp_code_status
  group by icustay_id
)
-- lowest pao2/fio2 for the first 4 days.. needed as an exclusion
, bg4days as
(
  select ce.icustay_id
    , min(PaO2FiO2Ratio) as pao2fio2ratio_min
  from dm_intime_outtime ce
  inner join icustays ie
    on ce.icustay_id = ie.icustay_id
  left join mp_bg_art
    on ie.hadm_id = mp_bg_art.hadm_id
    and ce.intime_hr <= mp_bg_art.charttime
    and ce.outtime_hr >= mp_bg_art.charttime
  group by ce.icustay_id
)

-- alcohol- use dependence related ICD-9
-- (291.X, 291.XX, 303.XX,  357.5, 425.5, 535.3X, 571.2, and 571.3)
-- ** paper says 305.XX but I'm sure it should be 305.0X
, icd_alc as
(
  select distinct hadm_id
  from diagnoses_icd
  where
     icd9_code like '291%'
     -- this length() is needed, e.g. 3051 is tobacco use *not* alcohol
  or (icd9_code like '303%' and length(icd9_code)=5)
  or icd9_code like '3050%'
  or icd9_code = '3575'
  or icd9_code = '4255'
  or icd9_code like '5353%'
  or icd9_code = '5712'
  or icd9_code = '5713'
)
, icd_aki as
(
  select distinct hadm_id
  from diagnoses_icd
  where icd9_code = '5849'
)
, icd_sah as
(
  select distinct hadm_id
  from diagnoses_icd
  where icd9_code = '430'
 -- the original study just specifies "852"
 -- 852 includes is subdural and extradural too, not just SAH
 -- also 852 is *not* a code on its own
 -- if we include 852*, then we obtain a much larger cohort than the authors report
    or icd9_code like '852%'
)
, icd_crf as
(
  select distinct hadm_id
  from diagnoses_icd
  where icd9_code like '585%'
)
, icd_sepsis as
(
  select distinct hadm_id
  from diagnoses_icd
  where icd9_code in ('99592','78552')
)
, cs as
(
  -- min code status
    select ce.icustay_id, min(cs.charttime) as censortime
    , ceil(extract(epoch from min(cs.charttime-ce.intime_hr) )/60.0/60.0) as censortime_hours
    from dm_intime_outtime ce
    inner join mp_code_status cs
    on ce.icustay_id = cs.icustay_id
    where (cmo+dnr+dni+dncpr+cmo_notes)>0
    group by ce.icustay_id
)
select
    ie.subject_id, ie.hadm_id, ie.icustay_id
  , ce.intime_hr as intime
  , ce.outtime_hr as outtime
  , round((cast(adm.admittime as date) - cast(pat.dob as date)) / 365.242, 4) as age
  , pat.gender
  , adm.ethnicity

  -- times
  , ceil(extract(epoch from (ce.outtime_hr- ce.intime_hr))/60.0/60.0) as dischtime_hours
  , ceil(extract(epoch from (adm.deathtime - ce.intime_hr))/60.0/60.0) as deathtime_hours
  , cs.censortime_hours


  -- other outcomes
  , ie.los as icu_los
  , extract(epoch from (adm.dischtime - adm.admittime))/60.0/60.0/24.0 as hosp_los

  -- mortality outcomes
  -- 48 post ICU admission -- TODO: check ~1700 pts die after other exclusions
  , case
      when adm.deathtime is not null and adm.deathtime <= ce.intime_hr + interval '48' hour
        then 1
      else 0
    end as death_48hr_post_icu_admit
  -- in ICU
	, case
      when adm.hospital_expire_flag = 1 and tr.outtime is not null
        then 1
      else 0
    end as death_icu
  -- in hospital
  , adm.HOSPITAL_EXPIRE_FLAG -- keeping this temporarily before refactor
  , adm.HOSPITAL_EXPIRE_FLAG as death_in_hospital
  -- 30-day post ICU admission
  , case
      -- died in hospital
      when adm.deathtime is not null and adm.deathtime <= ce.intime_hr + interval '30' day
        then 1
      -- died outside of hospital or during a later readmission to hospital
      when pat.dod is not null and pat.dod <= ce.intime_hr + interval '30' day
        then 1
      else 0
    end as death_30dy_post_icu_admit
  -- 30-day post ICU discharge
  , case
      -- died in hospital
      when adm.deathtime is not null and adm.deathtime <= ce.outtime_hr + interval '30' day
        then 1
      -- died outside of hospital or during a later readmission to hospital
      when pat.dod is not null and pat.dod <= ce.outtime_hr + interval '30' day
        then 1
      else 0
    end as death_30dy_post_icu_disch
  -- 30-day post hospital discharge
  , case
      -- died in hospital
      when adm.deathtime is not null and adm.deathtime <= adm.dischtime + interval '30' day
        then 1
      -- died outside of hospital or during a later readmission to hospital
      when pat.dod is not null and pat.dod <= adm.dischtime + interval '30' day
        then 1
      else 0
    end as death_30dy_post_hos_disch
  -- 6-month post hospital discharge
  , case
      -- died in hospital
      when adm.deathtime is not null and adm.deathtime <= adm.dischtime + interval '6' month
        then 1
      -- died outside of hospital or during a later readmission to hospital
      when pat.dod is not null and pat.dod <= adm.dischtime + interval '6' month
        then 1
      else 0
    end as death_6mo_post_hos_disch
  -- 1-year post hospital discharge
  , case
      -- died in hospital
      when adm.deathtime is not null and adm.deathtime <= adm.dischtime + interval '1' year
        then 1
      -- died outside of hospital or during a later readmission to hospital
      when pat.dod is not null and pat.dod <= adm.dischtime + interval '1' year
        then 1
      else 0
    end as death_1yr_post_hos_disch
  -- 2-year post hospital discharge
  , case
      -- died in hospital
      when adm.deathtime is not null and adm.deathtime <= adm.dischtime + interval '1' year
        then 1
      -- died outside of hospital or during a later readmission to hospital
      when pat.dod is not null and pat.dod <= adm.dischtime + interval '1' year
        then 1
      else 0
    end as death_2yr_post_hos_disch

  -- this isn't used in any study ... but is the more usual definition of 30-day mort, centered on *hospital admission*
  , case when pat.dod <= adm.admittime + interval '30' day then 1 else 0 end
      as death_30dy_post_hos_admit

  -- exclusions - these are applied for all cohorts
  , case when round((cast(adm.admittime as date) - cast(pat.dob as date)) / 365.242, 4) <= 15
      then 1
    else 0 end as exclusion_over_15
  , case when adm.HAS_CHARTEVENTS_DATA = 0 then 1
         when ie.intime is null then 1
         when ie.outtime is null then 1
         when ce.intime_hr is null then 1
         when ce.outtime_hr is null then 1
      else 0 end as exclusion_valid_data

  -- length of stay flags
  , case
      when (ce.outtime_hr-ce.intime_hr) < interval '4' hour then 1
    else 0 end as exclusion_stay_lt_4hr

  -- organ donor accounts
  , case when (
         (lower(diagnosis) like '%organ donor%' and deathtime is not null)
      or (lower(diagnosis) like '%donor account%' and deathtime is not null)
    ) then 1 else 0 end as exclusion_organ_donor

  -- regardless of the individual exclusions, we have some basic exclusions
  -- these are satisfied by all studies, and reduce the data size
  , case when round((cast(adm.admittime as date) - cast(pat.dob as date)) / 365.242, 4) <= 15 then 1
         when adm.HAS_CHARTEVENTS_DATA = 0 then 1
         when ie.intime is null then 1
         when ie.outtime is null then 1
         when ce.intime_hr is null then 1
         when ce.outtime_hr is null then 1
         when (ce.outtime_hr-ce.intime_hr) <= interval '4' hour then 1
         when lower(diagnosis) like '%organ donor%' and deathtime is not null then 1
         when lower(diagnosis) like '%donor account%' and deathtime is not null then 1
      else 0 end
    as excluded

  -- now we have individual study *inclusions*
  -- first some generic inclusions

  , case when round((cast(adm.admittime as date) - cast(pat.dob as date)) / 365.242, 4) > 16
      then 1
    else 0 end as inclusion_over_16
  , case when round((cast(adm.admittime as date) - cast(pat.dob as date)) / 365.242, 4) > 18
      then 1
    else 0 end as inclusion_over_18

  , case
      when (ce.outtime_hr-ce.intime_hr) >= interval '12' hour then 1
    else 0 end as inclusion_stay_ge_12hr
  , case
      when (ce.outtime_hr-ce.intime_hr) >= interval '17' hour then 1
    else 0 end as inclusion_stay_ge_17hr
  , case
      when (ce.outtime_hr-ce.intime_hr) >= interval '24' hour then 1
    else 0 end as inclusion_stay_ge_24hr
  , case
      when (ce.outtime_hr-ce.intime_hr) >= interval '48' hour then 1
    else 0 end as inclusion_stay_ge_48hr
  , case
      when (ce.outtime_hr-ce.intime_hr) < interval '500' hour then 1
    else 0 end as inclusion_stay_le_500hr

  , case when ROW_NUMBER() OVER (partition by ie.hadm_id order by ie.intime) = 1 then 1 else 0 end as inclusion_first_admission

  -- mimic-ii
  , case when ie.dbsource = 'carevue' then 1 else 0 end as inclusion_only_mimicii

  -- calvert2016using
  -- *in* the MICU (no mention of service), so we use careunit
  , case when ie.first_careunit = 'MICU' then 1 else 0 end as inclusion_only_micu
  -- calvert2016computational
  -- only alcoholic dependence patients
  , case when icd_alc.hadm_id is not null then 1 else 0 end as inclusion_non_alc_icd9
  -- must have obs
  , case when obs.heartrate>0
           or obs.sysbp>0
           or obs.meanbp>0
           or obs.resprate>0
           or obs.temp>0
           or obs.spo2>0
           or obs.gcs>0
           or obs.WBC>0
           or obs.ph>0
        then 1 else 0 end as inclusion_calvert2016_obs

  -- celi2012database
  , case when icd_aki.hadm_id is not null then 1 else 0 end as inclusion_aki_icd9
  , case when icd_sah.hadm_id is not null then 1 else 0 end as inclusion_sah_icd9

  -- ghassemi2014unfolding
  , case when wc.non_stop_words >= 100 then 1 else 0 end as inclusion_ge_100_non_stop_words

  -- ghassemi2015multivariate
  , case when dm_nn.number_of_notes > 6 then 1 else 0 end as inclusion_gt_6_notes

  -- grnarova2016neural
  -- from paper: "... with only one hospital admission"
  , case when count(ie.hadm_id) OVER (partition by ie.subject_id) = 1 then 1 else 0 end as inclusion_multiple_hadm

  -- harutyunyan2017multitask
  -- "excluded any hospital admission with multiple ICU stays or transfers between different ICU units or wards"
  -- looking at source code, it's count(icustay_id) > 1 for any hadm_id
  , case when count(ie.icustay_id) OVER (partition by ie.hadm_id) = 1 then 1 else 0 end as inclusion_multiple_icustay

  -- hoogendoorn2016prediction
  -- obs requirement as hug (below)

  -- hug2009icu
  -- need 1 obs for HR/GCS/Hct/BUN, not NSICU/TSICU, first ICU stay, full code, not on dialysis
  -- *EXCLUDE* CRF
  , case when obs.heartrate>0 and obs.gcs>0 and obs.hematocrit>0 and obs.bun>0 and obs.iv_rate>0 then 1 else 0 end as inclusion_hug2009_obs
  , case when serv.service_NMED=1 or serv.service_NSURG=1 or serv.service_TSURG=1 then 0 else 1 end as inclusion_hug2009_proposed_service
  -- hug's thesis states the service exclusions are:
  --    Neurosurgery patients (NSICU Service)
  --    Trauma patients (CSICU service)
  -- the below excl only works for carevue really, but we use the actual charted service here which is more consistent w/ old studies
  -- won't work for metavision though!
  , case when serv.nsicu_chart=1 or serv.csicu_chart=1 then 0 else 1 end as inclusion_hug2009_not_nsicu_csicu
  , case when cmo=1 or dnr=1 or dni=1 or dncpr=1 then 0 else 1 end as inclusion_full_code
  , case when dm_braindeath.brain_death=1 then 0 else 1 end as inclusion_not_brain_death
  , case when icd_crf.hadm_id is not null then 0 else 1 end as inclusion_not_crf
  -- received dialysis in the first 24 hours
  , case when dial.starttime < ce.intime_hr + interval '1' day then 0 else 1 end as inclusion_no_dialysis_first24hr

  -- lee2015customization
  -- Only MICU, SICU, CCU, CSRU, no missing data
  -- for mimic-ii, use the charted service for consistency with prev studies
  -- .. with the caveat that "MICU" and "SICU" are not valid strings, so we approximate the meaning
  , case when ie.dbsource != 'metavision'
          and (serv.medicine_chart=1 or serv.ccu_chart=1 or serv.surg_chart=1 or
               serv.msicu_chart=1 or serv.csru_chart=1)
              then 1
        -- rest of mimic-ii patients are excluded
        when ie.dbsource != 'metavision' then 0
        when (serv.service_MED=1 or serv.service_PSURG=1 or serv.service_SURG=1 or
              serv.service_CSURG=1 or serv.service_VSURG=1)
            then 1
        else 0 end
      as inclusion_lee2015_service

  -- lee2015personalization
  -- "Only ICU stays with complete data"
  -- they use SAPS-I vars, so we will enforce that (var is calculated later)
  , case when obs.saps_vars > 0 then 1 else 0 end as inclusion_has_saps

  -- lee2017patient
  -- must have SAPS, so above works

  -- lehman2012risk
  -- missing saps-i (see above)

  -- luo2016interpretable
  , case when ds.hadm_id is not null then 1 else 0 end as inclusion_no_disch_summary
  -- SAPS-II uses pao2/fio2 instead of hct, and doesn't use resp rate.. so we just use the "does the pt have saps-i" var here
  , case when obs.saps_vars > 0 then 1 else 0 end as inclusion_has_sapsii

  -- luo2016predicting
  -- Subset of Joshi2012 with "one day length of time series data"
  -- Joshi2012 is Hug2009
  -- We just apply >24hr + Hug's exclusions

  -- TODO: purushotham2017variational
  -- two reasons why this is a challenge:
  --  (1) a very atypical evaluation scheme and (2) very complex exclusion criteria
  -- AHRF patients as in Khemani2009, split into 4 datasets based on age
  -- (1) Patients were eligible if endotracheally intubated and mechanically ventilated
  -- , case when vdstart.starttime < ce.intime_hr + interval '4' day then 0 else 1 end as exclusion_not_vent_first96hr
  -- (2) and at least one PF ratio was less than 300 within 24 h after intubation.
  -- (3) Patients were excluded for evidence of cardiac disease
  -- (4) incomplete ventilation data.
  -- (5) All patients met three of four diagnostic criteria for ALI (acute onset, PF ratio \300, and no left ventricular dysfunction).
  -- (6) The presence of bilateral infiltrates on chest radiograph (fourth ALI criteria) was handled separately.
  -- (7) Finally, all patients with an endotracheal tube leak greater than 20% were excluded.

  -- ripoll2014sepsis
  -- Missing data, only sepsis patients (sepsis not defined) - we'll use explicit
  -- missing data == saps/sofa missing
  -- sepsis == explicit coding
  , case when icd_sepsis.hadm_id is not null then 1 else 0 end as inclusion_not_explicit_sepsis

  -- wojtusiak2017c
  -- Alive at hospital disch
  -- >65
  , case when adm.hospital_expire_flag = 0 then 1 else 0 end as inclusion_alive_hos_disch
  , case when round((cast(adm.admittime as date) - cast(pat.dob as date)) / 365.242, 4) > 65
      then 1
    else 0 end as inclusion_over_65

from icustays ie
inner join admissions adm
  on ie.hadm_id = adm.hadm_id
inner join patients pat
  on ie.subject_id = pat.subject_id
left join dm_intime_outtime ce
  on ie.icustay_id = ce.icustay_id
left join tr
	on ie.icustay_id = tr.icustay_id
	and tr.rn = 1
left join icd_alc
  on ie.hadm_id = icd_alc.hadm_id
left join icd_aki
  on ie.hadm_id = icd_aki.hadm_id
left join icd_sah
  on ie.hadm_id = icd_sah.hadm_id
left join icd_crf
  on ie.hadm_id = icd_crf.hadm_id
left join icd_sepsis
  on ie.hadm_id = icd_sepsis.hadm_id
left join dm_word_count wc
  on ie.hadm_id = wc.hadm_id
left join dm_number_of_notes dm_nn
  on ie.hadm_id = dm_nn.hadm_id
left join ds
  on ie.hadm_id = ds.hadm_id
left join dm_obs_count obs
  on ie.icustay_id = obs.icustay_id
left join fullcode
  on ie.icustay_id = fullcode.icustay_id
left join dm_braindeath
  on ie.hadm_id = dm_braindeath.hadm_id
left join dm_service serv
  on ie.icustay_id = serv.icustay_id
left join dm_dialysis_start dial
  on ie.icustay_id = dial.icustay_id
left join (select icustay_id, min(starttime) as starttime from ventdurations vd group by icustay_id) vdstart
  on ie.icustay_id = vdstart.icustay_id
left join cs
  on ie.icustay_id = cs.icustay_id
order by ie.icustay_id;
