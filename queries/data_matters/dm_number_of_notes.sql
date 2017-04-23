drop table if exists dm_number_of_notes cascade;
create table dm_number_of_notes as
select hadm_id, count(*) as number_of_notes
from noteevents
where category != 'Discharge summary'
group by hadm_id
order by hadm_id;
