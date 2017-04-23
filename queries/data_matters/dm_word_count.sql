-- creates a table with word counts for every note
-- the word counts are based off lexemes which exclude english stop words
-- see postgres docs about ts_tovector for details

-- in order to count the number of words, we use a postgres extension
-- installation is pretty simple! see here:
-- https://github.com/postgrespro/tsexact
-- provides the `poslen()` function

drop table if exists dm_word_count cascade;
create table dm_word_count as
select
  hadm_id, sum(poslen(to_tsvector(text))) as non_stop_words
from noteevents
group by hadm_id
order by hadm_id;
