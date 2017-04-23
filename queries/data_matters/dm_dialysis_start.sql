drop table if exists dm_dialysis_start cascade;
create table dm_dialysis_start as
with t1 as
(
select
  ce.icustay_id, min(charttime) as starttime
from chartevents ce
where itemid in
(
  -- ihd charted dialysis settings (not including access)
    146, 147, 152
  , 224154, 225977, 226499

  -- peritoneal
  , 225965, 225953, 225952, 225951, 225810


  -- Numeric values
  , 226499 -- | Hemodialysis Output                               | Dialysis                | chartevents        | Numeric
  , 224154 -- | Dialysate Rate                                    | Dialysis                | chartevents        | Numeric
  , 225810 -- | Dwell Time (Peritoneal Dialysis)                  | Dialysis                | chartevents        | Numeric
  , 227639 -- | Medication Added Amount  #2 (Peritoneal Dialysis) | Dialysis                | chartevents        | Numeric
  , 225183 -- | Current Goal                     | Dialysis | chartevents        | Numeric
  , 227438 -- | Volume not removed               | Dialysis | chartevents        | Numeric
  , 224191 -- | Hourly Patient Fluid Removal     | Dialysis | chartevents        | Numeric
  , 225806 -- | Volume In (PD)                   | Dialysis | chartevents        | Numeric
  , 225807 -- | Volume Out (PD)                  | Dialysis | chartevents        | Numeric
  , 228004 -- | Citrate (ACD-A)                  | Dialysis | chartevents        | Numeric
  , 228005 -- | PBP (Prefilter) Replacement Rate | Dialysis | chartevents        | Numeric
  , 228006 -- | Post Filter Replacement Rate     | Dialysis | chartevents        | Numeric
  , 224144 -- | Blood Flow (ml/min)              | Dialysis | chartevents        | Numeric
  , 224145 -- | Heparin Dose (per hour)          | Dialysis | chartevents        | Numeric
  , 224149 -- | Access Pressure                  | Dialysis | chartevents        | Numeric
  , 224150 -- | Filter Pressure                  | Dialysis | chartevents        | Numeric
  , 224151 -- | Effluent Pressure                | Dialysis | chartevents        | Numeric
  , 224152 -- | Return Pressure                  | Dialysis | chartevents        | Numeric
  , 224153 -- | Replacement Rate                 | Dialysis | chartevents        | Numeric
  , 224404 -- | ART Lumen Volume                 | Dialysis | chartevents        | Numeric
  , 224406 -- | VEN Lumen Volume                 | Dialysis | chartevents        | Numeric
  , 226457 -- | Ultrafiltrate Output             | Dialysis | chartevents        | Numeric

  -- crrt
  , 227290
  , 225802, 225803, 225805, 225809
)
or (ce.itemid = 582 and value in ('CAVH Start','CAVH D/C','CVVHD Start','CVVHD D/C','Hemodialysis st','Hemodialysis end'))
group by icustay_id
)
, t2 as
(
select
  oe.icustay_id, min(charttime) as starttime
from outputevents oe
where itemid in
(
  40425 -- dialysis output      | carevue    | outputevents       |
, 40507 -- Dialysis out         | carevue    | outputevents       |
, 41527 -- HEMODIALYSIS         | carevue    | outputevents       |
, 41250 -- HEMODIALYSIS OUT     | carevue    | outputevents       |
, 41374 -- Dialysis Out         | carevue    | outputevents       |
, 41417 -- Hemodialysis Out     | carevue    | outputevents       |
, 40881 -- Hemodialysis         | carevue    | outputevents       |
, 40910 -- PERITONEAL DIALYSIS
, 41016 -- hemodialysis out     | carevue    | outputevents       |
, 41112 -- Dialysys out         | carevue    | outputevents       |
, 42289 -- dialysis off         | carevue    | outputevents       |
, 42388 -- DIALYSIS OUTPUT      | carevue    | outputevents       |
, 42524 -- HemoDialysis         | carevue    | outputevents       |
, 42536 -- Dialysis output      | carevue    | outputevents       |
, 40386 -- hemodialysis         | carevue    | outputevents       |
, 41623 -- dialysate out        | carevue    | outputevents       |
, 41635 -- Hemodialysis removal | carevue    | outputevents       |
, 41713 -- dialyslate out       | carevue    | outputevents       |
, 41842 -- Dialysis Output.     | carevue    | outputevents       |
, 40624 -- dialysis             | carevue    | outputevents       |
, 41500 -- hemodialysis output  | carevue    | outputevents       |
, 43941 -- dialysis/intake      | carevue    | outputevents       | Free Form Intake
, 44199 -- HEMODIALYSIS O/P     | carevue    | outputevents       |
, 44901 -- Dialysis Removed     | carevue    | outputevents       |
, 44943 -- fluid removed dialys | carevue    | outputevents       |
, 42928 -- HEMODIALYSIS.        | carevue    | outputevents       |
, 42972 -- HEMODIALYSIS OFF     | carevue    | outputevents       |
, 43016 -- DIALYSIS TOTAL OUT   | carevue    | outputevents       |
, 43052 -- DIALYSIS REMOVED     | carevue    | outputevents       |
, 43098 -- hemodialysis crystal | carevue    | outputevents       | Free Form Intake
, 44567 -- Hemodialysis.        | carevue    | outputevents       |
, 46394 -- Peritoneal dialysis  | carevue    | outputevents       |
, 46713 -- KCL-10 MEQ-DIALYSIS  | carevue    | outputevents       | Free Form Intake
, 46741 -- dialysis removed     | carevue    | outputevents       |
, 40745 -- Dialysis             | carevue    | outputevents       |
, 40789 -- PD dialysate out     | carevue    | outputevents       |
, 44843 -- peritoneal dialysis  | carevue    | outputevents       | Free Form Intake
, 43687 -- crystalloid/dialysis | carevue    | outputevents       | Free Form Intake
, 45479 -- Dialysis In          | carevue    | outputevents       | Free Form Intake
, 46230 -- Dialysis 1.5% IN     | carevue    | outputevents       | Free Form Intake
, 46232 -- dialysis flush       | carevue    | outputevents       | Free Form Intake
, 44027 -- dialysis fluid off   | carevue    | outputevents       |
, 44085 -- DIALYSIS OFF         | carevue    | outputevents       |
, 44193 -- Dialysis.            | carevue    | outputevents       |
, 44216 -- Hemodialysis out     | carevue    | outputevents       |
, 44286 -- Dialysis indwelling  | carevue    | outputevents       | Free Form Intake
, 45828 -- Hemo dialysis out    | carevue    | outputevents       |
, 46464 -- Hemodialysis OUT     | carevue    | outputevents       |
, 42868 -- hemodialysis off     | carevue    | outputevents       |
, 43115 -- dialysis net         | carevue    | outputevents       |
, 41750 -- dialysis  out        | carevue    | outputevents       |
, 41829 -- HEMODIALYSIS OUTPUT  | carevue    | outputevents       |
, 41069 -- PD Dialysate Output  | carevue    | outputevents       |
, 40426 -- dialysis out         | carevue    | outputevents       |
, 40613 -- DIALYSIS OUT         | carevue    | outputevents       |
, 44845 -- Dialysis fluids      | carevue    | outputevents       | Free Form Intake
, 44857 -- dialysis- fluid off  | carevue    | outputevents       |
, 42464 -- hemodialysis ultrafe | carevue    | outputevents       |
)
group by icustay_id
)
, stg as
(
  select icustay_id, starttime
  from t1
  UNION
  select icustay_id, starttime
  from t2
)
select icustay_id, min(starttime) as starttime
from stg
group by icustay_id
order by icustay_id;
