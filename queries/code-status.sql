DROP TABLE IF EXISTS mp_code_status;
CREATE TABLE mp_code_status AS
with t1 as
(
  select icustay_id, charttime
  -- coalesce the values
  , max(case
      when value in ('Full Code','Full code') then 1
    else 0 end) as FullCode
  , max(case
      when value in ('Comfort Measures','Comfort measures only') then 1
    else 0 end) as CMO
  , max(case
      when value = 'CPR Not Indicate' then 1
    else 0 end) as DNCPR -- only in CareVue, i.e. only possible for ~60-70% of patients
  , max(case
      when value in ('Do Not Intubate','DNI (do not intubate)','DNR / DNI') then 1
    else 0 end) as DNI
  , max(case
      when value in ('Do Not Resuscita','DNR (do not resuscitate)','DNR / DNI') then 1
    else 0 end) as DNR
  from chartevents
  where itemid in (128, 223758)
  and value is not null
  and value != 'Other/Remarks'
  -- exclude rows marked as error
  AND error IS DISTINCT FROM 1
  group by icustay_id, charttime
)
-- examine the notes to determine if they were ever made cmo
, nnote as
(
  select
    hadm_id, charttime
    , max(case
        when substring(text from 'made CMO') != '' then 1
        when substring(lower(text) from 'cmo ordered') != '' then 1
        when substring(lower(text) from 'pt. is cmo') != '' then 1
        when substring(text from 'Code status:([ \r\n]+)Comfort measures only') != '' then 1
        --when substring(text from 'made CMO') != '' then 1
        --when substring(substring(text from '[^E]CMO') from 2 for 3) = 'CMO'
        --  then 1
        else 0
      end) as CMO
  from mimiciii.noteevents ne
  where category in ('Nursing/other','Nursing','Physician')
  and lower(text) like '%cmo%'
  group by hadm_id, charttime
)
select ie.subject_id
  , ie.hadm_id
  , ie.icustay_id

  , t1.charttime

  , t1.FullCode
  , t1.CMO
  , t1.DNR
  , t1.DNI
  , t1.DNCPR

  , 0 as CMO_notes
from icustays ie
left join t1
  on ie.icustay_id = t1.icustay_id
UNION
select ie.subject_id
  , ie.hadm_id
  , ie.icustay_id

  , nn.charttime

  , 0 as FullCode
  , 0 as CMO
  , 0 as DNR
  , 0 as DNI
  , 0 as DNCPR

  , nn.CMO as CMO_notes
from icustays ie
inner join nnote nn
  on ie.hadm_id = nn.hadm_id
  and nn.charttime between ie.intime and ie.outtime
ORDER BY icustay_id, charttime;
