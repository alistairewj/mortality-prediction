drop table if exists dm_braindeath cascade;
create table dm_braindeath as
select
  ne.hadm_id
  , max(case when lower(ne.text) like '%brain death%' then 1
      when lower(ne.text) like '%comatose%' then 1
      when lower(ne.text) like '%brain dead%' then 1
      -- typo is direct from hug thesis.. not sure if intentional
      when lower(ne.text) like '%brain steam dead%' then 1
      -- adding stem as well just in case
      when lower(ne.text) like '%brain stem dead%' then 1
      when lower(ne.text) like '%brain stem death%' then 1
    else 0 end) as brain_death
from noteevents ne
where ne.category = 'Discharge summary'
group by ne.hadm_id
order by ne.hadm_id;
