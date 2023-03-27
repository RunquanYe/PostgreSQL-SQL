create table ews.temp_2006 as
select
'2006' as "File year",
"STATE" as state,
"NAME" as name,
"ZIP5" as "zip code",
"EIN" as employer_id_number,
"FISYR" as fiscal_year
from ews.core2006pc;

select * from ews.core1995pc;

select count(*) from ews.core2006pc;

select count(*) from ews.temp_1998;

call ews.changevarcoltype('ews', 'temp_1992', '{}');

create table ews.temp_result as select * from ews.temp_1990 union select * from ews.temp_1991;

select * from ews.temp_result where ews.temp_result."File year" = '1991';

select "STATE", "NAME", "ZIP5" from ews.core1991pc where "EIN" = '232659';

drop table ews.temp_result, ews.temp_1990, ews.temp_1991;