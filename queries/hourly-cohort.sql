-- this query extracts the cohort and every possible hour they were in the ICU
-- this table is joined to other tables on ICUSTAY_ID and CHARTTIME
DROP TABLE IF EXISTS mp_hourly_cohort CASCADE;
CREATE TABLE mp_hourly_cohort as
select
  co.subject_id, co.hadm_id, co.icustay_id
  -- create integers for each charttime in hours from admission
  -- so 0 is admission time, 1 is one hour after admission, etc, up to ICU disch
  , generate_series
  (
    -- allow up to 24 hours before ICU admission (to grab labs before admit)
    -24,
    ceil(extract(EPOCH from outtime-intime)/60.0/60.0)::INTEGER
  ) as hr
from mp_cohort co
where co.excluded = 0
order by co.subject_id, co.hadm_id, co.icustay_id;
