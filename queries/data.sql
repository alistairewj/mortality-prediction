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
  , bg.SO2
  , bg.PO2, bg.PCO2
  -- also calculate AADO2
  -- , bg.AADO2
  --, AADO2_calc
  , bg.PaO2FiO2Ratio
  -- acid-base parameters
  , bg.PH
  , bg.BASEEXCESS
  , bg.BICARBONATE
  , bg.TOTALCO2

  -- blood count parameters
  , bg.HEMATOCRIT
  , bg.HEMOGLOBIN
  , bg.CARBOXYHEMOGLOBIN
  , bg.METHEMOGLOBIN

  -- chemistry
  , bg.CHLORIDE
  , bg.CALCIUM
  , bg.TEMPERATURE
  , bg.POTASSIUM
  , bg.SODIUM
  , bg.LACTATE
  , bg.GLUCOSE

  -- ventilation stuff that's sometimes input
  -- , INTUBATED, TIDALVOLUME, VENTILATIONRATE, VENTILATOR
  -- , bg.PEEP, O2Flow
  -- , REQUIREDO2

  -- labs
  , lab.ANIONGAP as ANIONGAP_lab
  , lab.ALBUMIN as ALBUMIN_lab
  , lab.BANDS as BANDS_lab
  , lab.BICARBONATE as BICARBONATE_lab
  , lab.BILIRUBIN as BILIRUBIN_lab
  , lab.CREATININE as CREATININE_lab
  , lab.CHLORIDE as CHLORIDE_lab
  , lab.GLUCOSE as GLUCOSE_lab
  , lab.HEMATOCRIT as HEMATOCRIT_lab
  , lab.HEMOGLOBIN as HEMOGLOBIN_lab
  , lab.LACTATE as LACTATE_lab
  , lab.PLATELET as PLATELET_lab
  , lab.POTASSIUM as POTASSIUM_lab
  , lab.PTT as PTT_lab
  , lab.INR as INR_lab
  , lab.PT as PT_lab
  , lab.SODIUM as SODIUM_lab
  , lab.BUN as BUN_lab
  , lab.WBC as WBC_lab
from mp_hourly_cohort mp
left join mp_vital vi
  on  mp.icustay_id = vi.icustay_id
  and mp.hr = vi.hr
left join mp_gcs gcs
  on  mp.icustay_id = gcs.icustay_id
  and mp.hr = gcs.hr
left join mp_bg_art bg
  on  mp.hadm_id = bg.hadm_id
  and mp.hr = bg.hr
left join mp_lab lab
  on  mp.hadm_id = lab.hadm_id
  and mp.hr = lab.hr
order by mp.subject_id, mp.hadm_id, mp.icustay_id, mp.hr;
