drop table if exists dm_service cascade;
create table dm_service as
with serv as
(
  select hadm_id, curr_service
    , ROW_NUMBER() over (PARTITION BY hadm_id ORDER BY transfertime) as rn
  from services
)
, chart_serv as
(
  select ce.icustay_id
  , max(case when ce.value in ('medicine','Med','med','MEDICINE','M','MICU','MED','MED            8') then 1 else 0 end) as medicine_chart
  -- ccu
  , max(case when ce.value in ('CCU','ccu','CCU/EP') then 1 else 0 end) as ccu_chart
  -- neuro (combined surgical or medical)
  , max(case when ce.value in ('NSICU','NSU','nsu','NEUROSURGURY','NSURG','neuro/sicu','N/SURG','NMED','NME','NEUROSURG') then 1 else 0 end) as neuro_chart


  -- csurg
  , max(case when ce.value in ('MICU/SICU','MSICU') then 1 else 0 end) as msicu_chart

  , max(case when ce.value in ('ORT','ORTHO') then 1 else 0 end) as ortho_chart
  , max(case when ce.value in ('GU') then 1 else 0 end) as gu_chart
  , max(case when ce.value in ('GYN') then 1 else 0 end) as gyn_chart
  , max(case when ce.value in ('PSU') then 1 else 0 end) as psu_chart
  , max(case when ce.value in ('ENT') then 1 else 0 end) as ent_chart
  , max(case when ce.value in ('OBS') then 1 else 0 end) as obs_chart
  , max(case when ce.value in ('CMED','cmed','CME','c-med','cardiology') then 1 else 0 end) as cmed_chart
  , max(case when ce.value in ('CSRU','CSURG','CRSU','CSU','csru','csurg','CSICU','csu','SCRU','CVI/CSRU','VASCULAR','VSURG','V SURG','VSU') then 1 else 0 end) as csru_chart
  , max(case when ce.value in ('SICU','SURG','SUR','surg','Surgery') then 1 else 0 end) as surg_chart
  , max(case when ce.value in ('DEN') then 1 else 0 end) as den_chart
  , max(case when ce.value in ('TRAUMA','trauma','Trauma','TSURG','TSU','T-SICU','TRA') then 1 else 0 end) as trauma_chart
  , max(case when ce.value in ('TRANSPLANT','Transplant') then 1 else 0 end) as transplant_chart
  , max(case when ce.value in ('OME') then 1 else 0 end) as omed_chart

  -- unable to guess, also only contains a handful of pts (<5 each)
  -- '',,
  -- 'TA','CFIRM','PCP',
  -- 'CE','MD','ICU','VU',

  -- redundant services for a study's exclusion criteria
  , max(case when ce.value = 'NSICU' then 1 else 0 end) as nsicu_chart
  , max(case when ce.value = 'CSICU' then 1 else 0 end) as csicu_chart

  from chartevents ce
  where itemid in (1125,919,224640)
  group by ce.icustay_id
)
SELECT
  ie.icustay_id

  -- charted services
  , cs.medicine_chart
  , cs.ccu_chart
  , cs.neuro_chart
  , cs.msicu_chart
  , cs.ortho_chart
  , cs.gu_chart
  , cs.gyn_chart
  , cs.psu_chart
  , cs.ent_chart
  , cs.obs_chart
  , cs.cmed_chart
  , cs.csru_chart
  , cs.surg_chart
  , cs.den_chart
  , cs.trauma_chart
  , cs.transplant_chart

  -- redundant to above (supersetted by above)
  -- used for some exclusions to precisely reproduce their criteria
  , cs.nsicu_chart
  , cs.csicu_chart
  
  , serv.curr_service
  -- reference is MED
  -- excluding (due to low sample size): DENT, PSYCH, OBS
  -- excluding newborns NB and NBB
  , case when serv.curr_service = 'MED'  then 1 else 0 end as service_MED
  , case when serv.curr_service = 'CMED'  then 1 else 0 end as service_CMED
  , case when serv.curr_service = 'OMED'  then 1 else 0 end as service_OMED
  , case when serv.curr_service = 'NMED'  then 1 else 0 end as service_NMED
  , case when serv.curr_service = 'NSURG' then 1 else 0 end as service_NSURG
  , case when serv.curr_service = 'TSURG' then 1 else 0 end as service_TSURG
  , case when serv.curr_service = 'CSURG' then 1 else 0 end as service_CSURG
  , case when serv.curr_service = 'VSURG' then 1 else 0 end as service_VSURG
  , case when serv.curr_service = 'ORTHO' then 1 else 0 end as service_ORTHO
  , case when serv.curr_service = 'PSURG' then 1 else 0 end as service_PSURG
  , case when serv.curr_service = 'SURG'  then 1 else 0 end as service_SURG

  , case when serv.curr_service = 'GU'    then 1 else 0 end as service_GU
  , case when serv.curr_service = 'GYN'   then 1 else 0 end as service_GYN
  , case when serv.curr_service = 'TRAUM' then 1 else 0 end as service_TRAUM
  , case when serv.curr_service = 'ENT'   then 1 else 0 end as service_ENT

  -- we aggregate some of these together due to low sample size
  , case when serv.curr_service in
      (
        'NSURG', 'TSURG', 'PSURG', 'SURG', 'ORTHO'
      ) then 1 else 0 end as service_ANY_NONCARD_SURG
  , case when serv.curr_service in
      (
        'CSURG', 'VSURG'
      ) then 1 else 0 end as service_ANY_CARD_SURG
from icustays ie
left join serv
  on ie.hadm_id = serv.hadm_id
  and serv.rn = 1
left join chart_serv cs
  on ie.icustay_id = cs.icustay_id
order by ie.icustay_id;
