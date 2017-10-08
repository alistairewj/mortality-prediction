-- FINAL DATA TABLE!
-- This combines (1) the base cohort with (2) materialized views to get patient data
-- The result is a table which is (N*Hn)xM
--  Rows: N patients times Hn hours for each patient (hours is variable)
--  Columns: M features
-- the "hr" column is the integer hour since ICU admission
-- it can be negative since some labs are measured before ICU admission
DROP TABLE IF EXISTS mp_data CASCADE;
CREATE TABLE mp_data as
select
  mp.subject_id, mp.hadm_id, mp.icustay_id
  , ih.hr
  -- vitals
  , vi.HeartRate
  , vi.SysBP
  , vi.DiasBP
  , vi.MeanBP
  , vi.RespRate
  , coalesce(bg.TEMPERATURE, vi.TempC) as tempc
  , coalesce(bg.SO2, vi.SpO2) as spo2
  , coalesce(lab.GLUCOSE,bg.GLUCOSE,vi.Glucose) as glucose

  -- gcs
  , gcs.GCS
  , gcs.GCSMotor
  , gcs.GCSVerbal
  , gcs.GCSEyes
  , gcs.EndoTrachFlag

  -- blood gases
  -- oxygen related parameters
  , bg.PO2 as bg_PO2
  , bg.PCO2 as bg_PCO2

  -- also calculate AADO2
  -- , bg.AADO2 as bg_AADO2
  --, AADO2_calc
  , bg.PaO2FiO2Ratio as bg_PaO2FiO2Ratio

  -- acid-base parameters
  , bg.PH as bg_PH
  , bg.BASEEXCESS as bg_BASEEXCESS
  , bg.TOTALCO2 as bg_TOTALCO2

  -- blood count parameters
  , bg.CARBOXYHEMOGLOBIN as bg_CARBOXYHEMOGLOBIN
  , bg.METHEMOGLOBIN as bg_METHEMOGLOBIN

  -- ventilation stuff that's sometimes input
  -- , INTUBATED, TIDALVOLUME, VENTILATIONRATE, VENTILATOR
  -- , bg.PEEP as bg_PEEP
  -- , O2Flow
  -- , REQUIREDO2

  -- labs
  , lab.ANIONGAP as ANIONGAP
  , lab.ALBUMIN as ALBUMIN
  , lab.BANDS as BANDS
  , coalesce(lab.BICARBONATE,bg.BICARBONATE) as BICARBONATE
  , lab.BILIRUBIN as BILIRUBIN
  , bg.CALCIUM as CALCIUM
  , lab.CREATININE as CREATININE
  , coalesce(lab.CHLORIDE, bg.CHLORIDE) as CHLORIDE
  , coalesce(lab.HEMATOCRIT,bg.HEMATOCRIT) as HEMATOCRIT
  , coalesce(lab.HEMOGLOBIN,bg.HEMOGLOBIN) as HEMOGLOBIN
  , coalesce(lab.LACTATE,bg.LACTATE) as LACTATE
  , lab.PLATELET as PLATELET
  , coalesce(lab.POTASSIUM, bg.POTASSIUM) as POTASSIUM
  , lab.PTT as PTT
  , lab.INR as INR
  -- , lab.PT as PT -- PT and INR are redundant
  , coalesce(lab.SODIUM, bg.SODIUM) as SODIUM
  , lab.BUN as BUN
  , lab.WBC as WBC

  , uo.UrineOutput
-- source from our "base" cohort
from mp_cohort mp
-- add in every hour for their icu stay
inner join icustay_hours ih
  on mp.icustay_id = ih.icustay_id
-- now left join to all the data tables using the hours
left join pivoted_vital vi
  on  ih.icustay_id = vi.icustay_id
  and ih.endtime - interval '1' hour < vi.charttime
  and ih.endtime >= vi.charttime
left join pivoted_gcs gcs
  on  ih.icustay_id = gcs.icustay_id
  and ih.endtime - interval '1' hour < gcs.charttime
  and ih.endtime >= gcs.charttime
left join pivoted_uo uo
  on  ih.icustay_id = uo.icustay_id
  and ih.endtime - interval '1' hour < uo.charttime
  and ih.endtime >= uo.charttime
left join pivoted_bg_art bg
  on  mp.hadm_id = bg.hadm_id
  and ih.endtime - interval '1' hour < bg.charttime
  and ih.endtime >= bg.charttime
left join pivoted_lab lab
  on  mp.hadm_id = lab.hadm_id
  and ih.endtime - interval '1' hour < lab.charttime
  and ih.endtime >= lab.charttime
where mp.excluded = 0
order by mp.subject_id, mp.hadm_id, mp.icustay_id, ih.hr;
