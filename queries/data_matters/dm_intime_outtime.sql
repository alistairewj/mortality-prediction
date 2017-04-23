drop table if exists dm_intime_outtime cascade;
create table dm_intime_outtime as
select icustay_id
  , min(charttime) as intime_hr
  , max(charttime) as outtime_hr
from chartevents ce
-- very loose join to admissions to ensure charttime is near patient admission
inner join admissions adm
  on ce.hadm_id = adm.hadm_id
  and ce.charttime between adm.admittime - interval '1' day and adm.dischtime + interval '1' day
where itemid in (211,220045)
group by icustay_id
order by icustay_id;
