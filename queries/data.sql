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
