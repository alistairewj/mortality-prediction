-- This query extracts weights for adult ICU patients with start/stop times
-- if an admission weight is given, then this is assigned from intime to outtime

DROP TABLE IF EXISTS weightdurations CASCADE;
CREATE TABLE weightdurations as
with adm_wt as
(
    SELECT
        ie.icustay_id
      , ie.intime as starttime
      -- we take the median value from roughly first day
      -- TODO: eliminate obvious outliers if there is a reasonable weight
      -- (e.g. weight of 180kg and 90kg would remove 180kg instead of taking the median)
      , valuenum as weight
    FROM icustays ie
    inner join chartevents c
      ON  c.icustay_id = ie.icustay_id
      AND c.valuenum IS NOT NULL
      AND c.itemid in (762,226512) -- Admit Wt
      AND c.valuenum != 0
      -- exclude rows marked as error
      AND c.error IS DISTINCT FROM 1
)
, dly_wt as
(
    SELECT
        ie.icustay_id
      , c.charttime as starttime
      , coalesce(
          LEAD(charttime) OVER (PARTITION BY ie.icustay_id ORDER BY c.charttime),
          -- we add a 2 hour "fuzziness" window
          ie.outtime + interval '2' hour
        ) as endtime
      , c.valuenum as weight
    FROM chartevents c
    inner join icustays ie
      on c.icustay_id = ie.icustay_id
    WHERE c.valuenum IS NOT NULL
      AND c.itemid in
      (
          763,224639 -- Daily Weight
      )
      AND c.valuenum != 0
      -- exclude rows marked as error
      AND c.error IS DISTINCT FROM 1
)
, wt as
(
  select
    adm_wt.icustay_id
    , adm_wt.starttime
    , cast(NULL as timestamp without time zone) as endtime
    , weight
  from adm_wt
  UNION
  SELECT
      dly_wt.icustay_id
    , dly_wt.starttime
    , dly_wt.endtime
    , dly_wt.weight
  from dly_wt
)
-- this table is the start/stop times from admit/daily weight in charted data
, wt1 as
(
  select
      ie.icustay_id
    , wt.starttime
    , case when wt.icustay_id is null then null
      else
        coalesce(wt.endtime,
        LEAD(wt.starttime) OVER (partition by ie.icustay_id order by wt.starttime),
          -- we add a 2 hour "fuzziness" window
        ie.outtime + interval '2' hour)
      end as endtime
    , wt.weight
  from icustays ie
  left join wt
    on ie.icustay_id = wt.icustay_id
)
-- if the intime for the patient is < the first charted daily weight
-- then we will have a "gap" at the start of their stay
-- to prevent this, we look for these gaps and backfill the first weight
-- this adds (153255-149657)=3598 rows, meaning this fix helps for 3598 icustay_id
, wt_fix as
(
  select ie.icustay_id
    -- we add a 2 hour "fuzziness" window
    , ie.intime - interval '2' hour as starttime
    , wt.starttime as endtime
    , wt.weight
  from icustays ie
  inner join
  -- the below subquery returns one row for each unique icustay_id
  -- the row contains: the first starttime and the corresponding weight
  (
    select wt1.icustay_id, wt1.starttime, wt1.weight
    from wt1
    inner join
      (
        select icustay_id, min(Starttime) as starttime
        from wt1
        group by icustay_id
      ) wt2
    on wt1.icustay_id = wt2.icustay_id
    and wt1.starttime = wt2.starttime
  ) wt
    on ie.icustay_id = wt.icustay_id
    and ie.intime < wt.starttime
)
, wt2 as
(
  select
      wt1.icustay_id
    , wt1.starttime
    , wt1.endtime
    , wt1.weight
  from wt1
  UNION
  SELECT
      wt_fix.icustay_id
    , wt_fix.starttime
    , wt_fix.endtime
    , wt_fix.weight
  from wt_fix
)
-- get more weights from echo - completes data for ~2500 patients
-- we only use echo data if there is *no* charted data
-- only ~762 patients remain with no weight data
, echo_hadm as
(
    select
        ie.icustay_id
        , ie.intime - interval '2' hour as starttime
        , ie.outtime + interval '2' hour as endtime
        , 0.453592*percentile_cont(0.5) WITHIN GROUP (ORDER BY weight) as Weight_EchoInHosp
    from echodata ec
    inner join icustays ie
        on ec.hadm_id = ie.hadm_id
    where ec.weight is not null
    group by ie.icustay_id, ie.intime, ie.outtime
)
select
    wt2.icustay_id
  , coalesce(wt2.starttime, ec.starttime) as starttime
  , coalesce(wt2.endtime, ec.endtime) as endtime
  , coalesce(wt2.weight, ec.weight_echoinhosp) as weight
from wt2
left join echo_hadm ec
  on wt2.icustay_id = ec.icustay_id
order by icustay_id, starttime, endtime;
