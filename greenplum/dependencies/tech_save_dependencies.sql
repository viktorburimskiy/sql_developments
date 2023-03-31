CREATE TABLE tech.tech_save_dependencies (
    table_id bigint,
    table_nm text,
    view_id bigint,
    dep_level integer,
    schema_name text,
    view_name text,
    sql_text text,
    view_owner text,
    type_sql text
)
WITH (appendonly='false');

