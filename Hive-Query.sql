create database disaster;
use disaster;

create table events (
    Entity string,
    Year string,
    Disasters int
) row format delimited fields terminated by ',' stored as textfile;

create table events_preprocessed like events;

create table entity_count (Entity string, total_count int) 
row format delimited fields terminated by ',' stored as textfile;

create table entity_year_range (entity string, year_range string, total_count int) 
row format delimited fields terminated by ',' stored as textfile;

alter table events set tblproperties("skip.header.line.count"="1");

load data inpath '/user/disaster/disaster-events.csv' overwrite into table events;

select * from events limit 10;

insert overwrite table events_preprocessed
select * from events
where entity != 'All disasters' 
and entity != 'All disasters excluding earthquakes' 
and entity != 'All disasters excluding extreme temperature';

select * from events_preprocessed limit 10;

insert overwrite table entity_count
select entity, sum(disasters) from events_preprocessed group by entity;

select * from entity_count;

insert overwrite table entity_year_range
select entity, 
case 
    when cast(year as int) >= 1900 and cast(year as int) < 1910 then '1900s' 
    when cast(year as int) >= 1910 and cast(year as int) < 1920 then '1910s'
    when cast(year as int) >= 1920 and cast(year as int) < 1930 then '1920s'
    when cast(year as int) >= 1930 and cast(year as int) < 1940 then '1930s'
    when cast(year as int) >= 1940 and cast(year as int) < 1950 then '1940s'
    when cast(year as int) >= 1950 and cast(year as int) < 1960 then '1950s'
    when cast(year as int) >= 1960 and cast(year as int) < 1970 then '1960s'
    when cast(year as int) >= 1970 and cast(year as int) < 1980 then '1970s'
    when cast(year as int) >= 1980 and cast(year as int) < 1990 then '1980s'
    when cast(year as int) >= 1990 and cast(year as int) < 2000 then '1990s'
    when cast(year as int) >= 2000 and cast(year as int) < 2010 then '2000s'
    when cast(year as int) >= 2010 and cast(year as int) < 2020 then '2010s'
    when cast(year as int) >= 2020 then '2020s'
end as year_range, 
sum(disasters) 
from events_preprocessed 
group by entity, year_range;

select * from entity_year_range limit 10;

