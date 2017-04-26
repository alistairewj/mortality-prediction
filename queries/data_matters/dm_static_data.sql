-- demographics, etc
DROP TABLE IF EXISTS dm_static_data CASCADE;
CREATE TABLE dm_static_data AS
with serv as
(
  select hadm_id, curr_service
    , ROW_NUMBER() over (PARTITION BY hadm_id ORDER BY transfertime) as rn
  from services
)
SELECT
  co.subject_id, co.hadm_id, co.icustay_id

  -- patient level factors
  , case when pat.gender = 'M' then 1 else 0 end as is_male

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

  -- hospital level factors
  , ROUND( (CAST(co.intime AS DATE) - CAST(pat.dob AS DATE))  / 365.242, 4) AS age

  -- ethnicity flags
  -- , case when adm.ethnicity in
  -- (
  --      'WHITE' --  40996
  --    , 'WHITE - RUSSIAN' --    164
  --    , 'WHITE - OTHER EUROPEAN' --     81
  --    , 'WHITE - BRAZILIAN' --     59
  --    , 'WHITE - EASTERN EUROPEAN' --     25
  -- ) then 1 else 0 end as race_white
  , case when adm.ethnicity in
  (
        'BLACK/AFRICAN AMERICAN' --   5440
      , 'BLACK/CAPE VERDEAN' --    200
      , 'BLACK/HAITIAN' --    101
      , 'BLACK/AFRICAN' --     44
      , 'CARIBBEAN ISLAND' --      9
  ) then 1 else 0 end as race_black
  , case when adm.ethnicity in
  (
    'HISPANIC OR LATINO' --   1696
  , 'HISPANIC/LATINO - PUERTO RICAN' --    232
  , 'HISPANIC/LATINO - DOMINICAN' --     78
  , 'HISPANIC/LATINO - GUATEMALAN' --     40
  , 'HISPANIC/LATINO - CUBAN' --     24
  , 'HISPANIC/LATINO - SALVADORAN' --     19
  , 'HISPANIC/LATINO - CENTRAL AMERICAN (OTHER)' --     13
  , 'HISPANIC/LATINO - MEXICAN' --     13
  , 'HISPANIC/LATINO - COLOMBIAN' --      9
  , 'HISPANIC/LATINO - HONDURAN' --      4
  ) then 1 else 0 end as race_hispanic
  , case when adm.ethnicity in
  (
      'ASIAN' --   1509
    , 'ASIAN - CHINESE' --    277
    , 'ASIAN - ASIAN INDIAN' --     85
    , 'ASIAN - VIETNAMESE' --     53
    , 'ASIAN - FILIPINO' --     25
    , 'ASIAN - CAMBODIAN' --     17
    , 'ASIAN - OTHER' --     17
    , 'ASIAN - KOREAN' --     13
    , 'ASIAN - JAPANESE' --      7
    , 'ASIAN - THAI' --      4
  ) then 1 else 0 end as race_asian
  , case when adm.ethnicity in
  (

      'UNKNOWN/NOT SPECIFIED' --   4523
    , 'OTHER' --   1512
    , 'UNABLE TO OBTAIN' --    814
    , 'PATIENT DECLINED TO ANSWER' --    559
    , 'MULTI RACE ETHNICITY' --    130
    , 'PORTUGUESE' --     61
    , 'AMERICAN INDIAN/ALASKA NATIVE' --     51
    , 'MIDDLE EASTERN' --     43
    , 'NATIVE HAWAIIAN OR OTHER PACIFIC ISLANDER' --     18
    , 'SOUTH AMERICAN' --      8
    , 'AMERICAN INDIAN/ALASKA NATIVE FEDERALLY RECOGNIZED TRIBE' --      3
  ) then 1 else 0 end as race_other

  , case when adm.ADMISSION_TYPE in ('URGENT','EMERGENCY') then 1 else 0 end as emergency_admission

  , ht.Height
  , wt.Weight
  , wt.Weight / (ht.Height/100*ht.Height/100) as bmi

FROM dm_cohort co
inner join icustays ie
  on co.icustay_id = ie.icustay_id
INNER JOIN admissions adm
  ON co.hadm_id = adm.hadm_id
INNER JOIN patients pat
  ON co.subject_id = pat.subject_id
left join heightfirstday ht
  on co.icustay_id = ht.icustay_id
left join weightfirstday wt
  on co.icustay_id = wt.icustay_id
left join serv
  on co.hadm_id = serv.hadm_id
  and serv.rn = 1
where co.excluded = 0
ORDER BY co.subject_id, co.hadm_id, co.icustay_id;
