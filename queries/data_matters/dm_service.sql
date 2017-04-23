with serv as
(
  select hadm_id, curr_service
    , ROW_NUMBER() over (PARTITION BY hadm_id ORDER BY transfertime) as rn
  from services
)

SELECT
  ie.icustay_id

  -- TODO:chart service


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
