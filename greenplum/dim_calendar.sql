CREATE TABLE tst.dim_calendar (
	calendar_id  int4 not null,
	calendar_dt date,
	day_of_week int2,
	day_of_month int2,
	day_of_year int2,
	weekday_of_month int2,
	weekday_short_name text,
	week_of_month int2,
	week_of_year int2 ,
	week_first_dt date,
	week_last_dt date,
	month_short_name text,
	month_of_quarter int2,
	month_of_year int2,
	month_first_dt date,
	month_last_dt date ,
	quarter_of_year int2,
	quarter_first_dt date,
	quarter_last_dt date,
	year_of_calendar int2,
	workday_of_month int2,
	workday_of_quarter int2,
	is_end_of_week bool,
	is_end_of_month bool,
	is_end_of_quarter bool,
	is_short_workday bool,
	is_holiday bool
)
WITH (
	appendonly=true
)
DISTRIBUTED BY (calendar_id);

--Описание объектов
comment on table tst.dim_calendar is 'Календарь';
comment on column  tst.dim_calendar.calendar_id is 'Id даты в формате YYYY*100 + MM';
comment on column  tst.dim_calendar.calendar_dt is 'Дата';
comment on column  tst.dim_calendar.day_of_week is 'Номер дня недели';
comment on column  tst.dim_calendar.day_of_month is 'День месяца';
comment on column  tst.dim_calendar.day_of_year is 'День года';
comment on column  tst.dim_calendar.weekday_of_month is 'Порядковый номер дня недели в месяце';
comment on column  tst.dim_calendar.weekday_short_name is 'Краткое наименование дня недели';
comment on column  tst.dim_calendar.week_of_month is 'Номер недели в месяце (с учетом предыдущего года)';
comment on column  tst.dim_calendar.week_of_year is 'Номер недели в году (с учетом предыдущего года)';
comment on column  tst.dim_calendar.week_first_dt is 'Первый день недели';
comment on column  tst.dim_calendar.week_last_dt is 'Последний день недели';
comment on column  tst.dim_calendar.month_short_name is 'Краткое наименование месяца';
comment on column  tst.dim_calendar.month_of_quarter is 'Номер месяца в квартале';
comment on column  tst.dim_calendar.month_of_year is 'Месяц';
comment on column  tst.dim_calendar.month_first_dt is 'Первый день месяца';
comment on column  tst.dim_calendar.month_last_dt is 'Последний день месяца';
comment on column  tst.dim_calendar.quarter_of_year is 'Номер квартала в году';
comment on column  tst.dim_calendar.quarter_first_dt is 'Первый день квартала';
comment on column  tst.dim_calendar.quarter_last_dt is 'Последний день квартала';
comment on column  tst.dim_calendar.year_of_calendar is 'Год';
comment on column  tst.dim_calendar.workday_of_month is 'Рабочий день в месяце';
comment on column  tst.dim_calendar.workday_of_quarter is 'Рабочий день в квартале';
comment on column  tst.dim_calendar.is_end_of_week is 'Признак последнего дня недели';
comment on column  tst.dim_calendar.is_end_of_month is 'Признак последнего дня месяца';
comment on column  tst.dim_calendar.is_end_of_quarter is 'Признак последнего дня квартала';
comment on column  tst.dim_calendar.is_short_workday is 'Признак сокращенного рабочего дня';
comment on column  tst.dim_calendar.is_holiday is 'Признак выходного дня';


--Заполнение справочника (по умолчанию с 01.01.1900 по 31.12.9999
insert into tst.dim_calendar (
	calendar_id
	,calendar_dt
	,day_of_week
	,day_of_month
	,day_of_year
	,weekday_of_month
	,weekday_short_name
	,week_of_month
	,week_of_year
	,week_first_dt
	,week_last_dt
	,month_short_name
	,month_of_quarter
	,month_of_year
	,month_first_dt
	,month_last_dt
	,quarter_of_year
	,quarter_first_dt
	,quarter_last_dt
	,year_of_calendar
	,workday_of_month
	,workday_of_quarter
	,is_end_of_week
	,is_end_of_month
	,is_end_of_quarter
	,is_short_workday
	,is_holiday)
select
	(extract('year' from dt) * 100 + extract('month' from dt))::int4 as calendar_id
	,dt::date as calendar_dt
	,extract('isodow' from dt)::int2 as day_of_week
	,extract('day' from dt)::int2 as day_of_month
	,extract('doy' from dt)::int2 as day_of_year
	,to_char(dt, 'w')::int2 as weekday_of_month
	,case extract('isodow' from dt)
		when 1 then 'Пн'
		when 2 then 'Вт'
		when 3 then 'Ср'
		when 4 then 'Чт'
		when 5 then 'Пт'
		when 6 then 'Сб'
		when 7 then 'Вс'
	end as weekday_short_name
	,dense_rank() over (partition by date_trunc('month', dt) order by extract('week' from dt))::int2 as week_of_month
	,extract('week' from dt)::int2 as week_of_year
	,date_trunc('week', dt)::date as week_first_dt
	,(date_trunc('week', dt) + interval '6 day')::date as week_last_dt
	,case extract('month' from dt)
		when 1 then 'Янв'
		when 2 then 'Фев'
		when 3 then 'Мар'
		when 4 then 'Апр'
		when 5 then 'Май'
		when 6 then 'Июн'
		when 7 then 'Июл'
		when 8 then 'Авг'
		when 9 then 'Сен'
		when 10 then 'Окт'
		when 11 then 'Ноя'
		when 12 then 'Дек'
	end as month_short_name
	,dense_rank() over (partition by extract('quarter' from dt) order by date_trunc('month', dt))::int2 as month_of_quarter
	,extract('month' from dt)::int2 as month_of_year
	,date_trunc('month', dt)::date as month_first_dt
	,(date_trunc('month', dt) + interval '1 month - 1 day')::date as month_last_dt
	,extract('quarter' from dt)::int2 as quarter_of_year
	,date_trunc('quarter', dt)::date as quarter_first_dt
	,(date_trunc('quarter', dt) + interval '3 month - 1 day')::date as quarter_last_dt
	,extract('year' from dt)::int2 as year_of_calendar
	,null::int2 as workday_of_month
	,null::int2 as workday_of_quarter
	,dt = date_trunc('week', dt)::date + 6 as is_end_of_week
	,dt = (date_trunc('month', dt)::date + interval '1 month - 1 day')::date as is_end_of_month
	,dt = (date_trunc('quarter', dt)::date + interval '3 month - 1 day')::date as is_end_of_quarter
	,null::bool as is_short_workday
	,null::bool as is_holiday
from generate_series(date '1900-01-01', date '9999-12-31', interval '1 day') as t(dt)
order by dt;

