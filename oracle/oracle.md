## Oracle query

#### Создание табличных функций:
```sql
-- тип "recordset" с набором атрибутов, которые будет возвращать наша функция
create or replace type record_part_type as object (
	table_owner varchar2 (128),
	table_name varchar2 (128), 
	partition_name varchar2 (128), 
	last_analyzed date
);

-- табличный тип на основе нашего recordset'а 
create or replace type table_part_type as table of record_part_type; 

--сама табличная функция
create or replace function get_partition_table(p_table varchar2) 
return table_part_type
is
    my_table table_part_type ; -- создаем пустую таблицу
begin
	
	my_table := table_part_type(); 
	
	select record_part_type(table_owner, table_name, partition_name, last_analyzed)
    bulk collect into my_table
    from (SELECT
			table_owner,
			table_name,
			partition_name,
			last_analyzed
		FROM ALL_TAB_PARTITIONS
		WHERE table_name = p_table);

    return my_table;
end;

--вызов
SELECT * FROM TABLE (get_partition_table('TEST_FULL'))

```


#### Работа с партициями:
```sql
-- создание партиционированной таблицы с опцией параллельной обработки
create table my_user.test_full
(
    job_id number(15,0),
    id number(10,0),
    text varchar2(200),
    create_dttm timestamp(6),
    drp_batch_id number(10,0) generated always as (mod(id,10)) virtual
)
tablespace users
partition by list (job_id) automatic 
    (
        partition p0 values (0) segment creation deferred
    )
parallel;

--добавляем партицию p1 с ключом 1
alter table my_user.test_full add partition p1 values (1);

--удаляем партицию по ключу 1
alter table my_user.test_full drop partition for (1);

--удаляем партицию по имени партиции
alter table my_user.test_full drop partition p1;

--подмена таблицы и партиции по ключу 1 с валидацией атрибутов
alter table my_user.test_full exchange partition for(1) with table my_user.test_full_temp with validation;

--очистка партиции
alter table my_user.test_full truncate partition p1;
```

#### Статистика:
```sql
-- получить информацию по статистике для таблицы
select table_name
	, partition_name
	, partition_position
	, num_rows
	, blocks
	, last_analyzed 
from user_tab_statistics
where table_name = 'TEST_FULL'
order by table_name, partition_position;

-- сбор статистики
begin
  dbms_stats.gather_table_stats(owner => 'schema_name'
                              , tabname => 'table_name'
                              , partname => 'partition_name'
                              , estimate_percent => 10);
end;



 
```
