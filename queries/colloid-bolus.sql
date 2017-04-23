DROP TABLE IF EXISTS mp_colloid_bolus CASCADE;
CREATE TABLE mp_colloid_bolus AS
with t1 as
(
  select
    co.icustay_id
  , ceil(extract(EPOCH from mv.starttime-co.intime)/60.0/60.0)::smallint as hr
  -- standardize the units to millilitres
  -- also metavision has floating point precision.. but we only care down to the mL
  , round(case
      when mv.amountuom = 'L'
        then mv.amount * 1000.0
      when mv.amountuom = 'ml'
        then mv.amount
    else null end) as amount
  from mp_cohort co
  inner join inputevents_mv mv
  on co.icustay_id = mv.icustay_id
  and mv.itemid in
  (
    220864, --	Albumin 5%	7466 132 7466
    220862, --	Albumin 25%	9851 174 9851
    225174, --	Hetastarch (Hespan) 6%	82 1 82
    225795,  --	Dextran 40	38 3 38
    225796 --  Dextran 70
    -- below ITEMIDs not in use
   -- 220861 | Albumin (Human) 20%
   -- 220863 | Albumin (Human) 4%
  )
  where mv.statusdescription != 'Rewritten'
  and
  -- in MetaVision, these ITEMIDs never appear with a null rate
  -- so it is sufficient to check the rate is > 100
    (
      (mv.rateuom = 'mL/hour' and mv.rate > 99)
      OR (mv.rateuom = 'mL/min' and mv.rate > (99/60.0))
      OR (mv.rateuom = 'mL/kg/hour' and (mv.rate*mv.patientweight) > 99)
    )
)
, t2 as
(
  select
    co.icustay_id
  , ceil(extract(EPOCH from cv.charttime-co.intime)/60.0/60.0)::smallint as hr
  -- carevue always has units in millilitres (or null)
  , round(cv.amount) as amount
  from mp_cohort co
  inner join inputevents_cv cv
  on co.icustay_id = cv.icustay_id
  and cv.itemid in
  (
   30008 --	Albumin 5%
  ,30181 -- Serum Albumin 5%
  ,40548 --	ALBUMIN
  ,45403 --	albumin
  ,46564 -- Albumin
  ,44203 --	Albumin 12.5%
  ,42832 --	albumin 12.5%
  ,43237 -- 25% Albumin
  ,43353 -- Albumin (human) 25%
  ,30009 --	Albumin 25%

  ,30012 --	Hespan
  ,46313 --	6% Hespan

  ,30011 -- Dextran 40
  ,40033 --	DEXTRAN
  ,42731 -- Dextran40 10%
  ,42975 --	DEXTRAN DRIP
  ,42944 --	dextran
  ,46336 --	10% Dextran 40/D5W
  ,46729 --	Dextran
  ,45410 --	10% Dextran 40
  )
  where cv.amount > 99
  and cv.amount < 2000
)
-- some colloids are charted in chartevents
, t3 as
(
  select
    co.icustay_id
  , ceil(extract(EPOCH from ce.charttime-co.intime)/60.0/60.0)::smallint as hr
  -- carevue always has units in millilitres (or null)
  , round(ce.valuenum) as amount
  from mp_cohort co
  inner join chartevents ce
  on co.icustay_id = ce.icustay_id
  and ce.itemid in
  (
      2510 --	DEXTRAN LML 10%
    , 3087 --	DEXTRAN 40  10%
    , 6937 --	Dextran
    , 3087 -- | DEXTRAN 40  10%
    , 3088 --	DEXTRAN 40%
  )
  where ce.valuenum is not null
  and ce.valuenum > 99
  and ce.valuenum < 2000
)
select
    icustay_id
  , hr
  , sum(amount) as colloid_bolus
from t1
-- just because the rate was high enough, does *not* mean the final amount was
where amount > 99
group by t1.icustay_id, t1.hr
UNION
select
    icustay_id
  , hr
  , sum(amount) as colloid_bolus
from t2
group by t2.icustay_id, t2.hr
UNION
select
    icustay_id
  , hr
  , sum(amount) as colloid_bolus
from t3
group by t3.icustay_id, t3.hr
order by icustay_id, hr;
