-- this combines all views to get all features at all time points
--
DROP TABLE IF EXISTS mp_data CASCADE;
CREATE TABLE mp_data as
select
  mp.subject_id, mp.hadm_id, mp.icustay_id
  , mp.hr
  -- vitals
  , vi.HeartRate
  , vi.SysBP
  , vi.DiasBP
  , vi.MeanBP
  , vi.RespRate
  , vi.TempC
  , vi.SpO2
  , vi.Glucose as glucose_chart
  -- gcs
  , gcs.GCS
  , gcs.GCSMotor
  , gcs.GCSVerbal
  , gcs.GCSEyes
  , gcs.EndoTrachFlag
  -- blood gases
  -- oxygen related parameters
  , bg.SO2 as bg_SO2
  , bg.PO2 as bg_PO2
  , bg.PCO2 as bg_PCO2
  -- also calculate AADO2
  -- , bg.AADO2 as bg_AADO2
  --, AADO2_calc
  , bg.PaO2FiO2Ratio as bg_PaO2FiO2Ratio
  -- acid-base parameters
  , bg.PH as bg_PH
  , bg.BASEEXCESS as bg_BASEEXCESS
  , bg.BICARBONATE as bg_BICARBONATE
  , bg.TOTALCO2 as bg_TOTALCO2

  -- blood count parameters
  , bg.HEMATOCRIT as bg_HEMATOCRIT
  , bg.HEMOGLOBIN as bg_HEMOGLOBIN
  , bg.CARBOXYHEMOGLOBIN as bg_CARBOXYHEMOGLOBIN
  , bg.METHEMOGLOBIN as bg_METHEMOGLOBIN

  -- chemistry
  , bg.CHLORIDE as bg_CHLORIDE
  , bg.CALCIUM as bg_CALCIUM
  , bg.TEMPERATURE as bg_TEMPERATURE
  , bg.POTASSIUM as bg_POTASSIUM
  , bg.SODIUM as bg_SODIUM
  , bg.LACTATE as bg_LACTATE
  , bg.GLUCOSE as bg_GLUCOSE

  -- ventilation stuff that's sometimes input
  -- , INTUBATED, TIDALVOLUME, VENTILATIONRATE, VENTILATOR
  -- , bg.PEEP as bg_PEEP
  -- , O2Flow
  -- , REQUIREDO2

  -- labs
  , lab.ANIONGAP as ANIONGAP
  , lab.ALBUMIN as ALBUMIN
  , lab.BANDS as BANDS
  , lab.BICARBONATE as BICARBONATE
  , lab.BILIRUBIN as BILIRUBIN
  , lab.CREATININE as CREATININE
  , lab.CHLORIDE as CHLORIDE
  , lab.GLUCOSE as GLUCOSE
  , lab.HEMATOCRIT as HEMATOCRIT
  , lab.HEMOGLOBIN as HEMOGLOBIN
  , lab.LACTATE as LACTATE
  , lab.PLATELET as PLATELET
  , lab.POTASSIUM as POTASSIUM
  , lab.PTT as PTT
  , lab.INR as INR
  , lab.PT as PT
  , lab.SODIUM as SODIUM
  , lab.BUN as BUN
  , lab.WBC as WBC

  , uo.UrineOutput
from mp_hourly_cohort mp
left join mp_vital vi
  on  mp.icustay_id = vi.icustay_id
  and mp.hr = vi.hr
left join mp_gcs gcs
  on  mp.icustay_id = gcs.icustay_id
  and mp.hr = gcs.hr
left join mp_uo uo
  on  mp.icustay_id = uo.icustay_id
  and mp.hr = uo.hr
left join mp_bg_art bg
  on  mp.hadm_id = bg.hadm_id
  and mp.hr = bg.hr
left join mp_lab lab
  on  mp.hadm_id = lab.hadm_id
  and mp.hr = lab.hr
order by mp.subject_id, mp.hadm_id, mp.icustay_id, mp.hr;
