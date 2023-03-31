CREATE TABLE tech.tech_logs (
    id bigint NOT NULL,
    flow_id bigint NOT NULL,
    proc_name character varying(64),
    param text,
    sql_query text,
    run_dttm_st timestamp with time zone,
    run_dttm_end timestamp with time zone,
    run_dttm_all interval,
    str_count bigint
)
WITH (appendonly='false');

CREATE SEQUENCE tech.tech_logs_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

