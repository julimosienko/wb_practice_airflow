create table if not exists default.tareLoad
(
    tare_id       UInt64,
    is_load       UInt8,
    dt            DateTime,
    office_id     UInt32,
    dst_office_id UInt32 comment 'load>0, unload=0'
)
    engine = MergeTree()
    PARTITION BY toYYYYMMDD(dt)
    ORDER BY (is_load, tare_id)
    TTL toStartOfDay(dt) + toIntervalDay(3)
    SETTINGS ttl_only_drop_parts = 1, index_granularity = 8192, merge_with_ttl_timeout = 36000;

create database report;

create table if not exists report.load_qty_tare_hour
(
    dt_hour         DateTime64(3),
    office_id       UInt32,
    qty_load        UInt32,
    dt_load         DateTime materialized now()
)
engine = ReplacingMergeTree()
partition by toYYYYMMDD(dt_hour)
order by (dt_hour, office_id)
ttl toStartOfDay(dt_hour) + interval 30 day;

select *
from report.load_qty_tare_hour;
