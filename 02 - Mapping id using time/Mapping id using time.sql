
drop table if exists map_id_time_union;
create table test.map_id_time_union
stored as parquet
as
select id, package_order, username, warehouse_id, type, time_stamp, 'log_package' as type_log
from package_logs_table
where type in ('up', 'down')
union
select id, null, username, warehouse_id, type, time_stamp, 'session' as type_log
from sessions_table

--------------------------------------------------------------------------------------

drop table if exists map_id_time_mapped;
create table test.map_id_time_mapped
stored as parquet
as
select id, package_order, username, warehouse_id, type, time_stamp, map_id as session_id, map_time_stamp
from (
    select
        case
            when type_log = 'session' then id
            when first_id is null then last_id
            when last_id is null then first_id
            when unix_timestamp(time_stamp) - unix_timestamp(first_time_stamp) <= unix_timestamp(last_time_stamp) - unix_timestamp(time_stamp) then first_id
            when unix_timestamp(time_stamp) - unix_timestamp(first_time_stamp) >= unix_timestamp(last_time_stamp) - unix_timestamp(time_stamp) then last_id
        end as map_id,
        case
            when type_log = 'session' then time_stamp
            when first_time_stamp is null then last_time_stamp
            when last_time_stamp is null then first_time_stamp
            when unix_timestamp(time_stamp) - unix_timestamp(first_time_stamp) <= unix_timestamp(last_time_stamp) - unix_timestamp(time_stamp) then first_time_stamp
            when unix_timestamp(time_stamp) - unix_timestamp(first_time_stamp) >= unix_timestamp(last_time_stamp) - unix_timestamp(time_stamp) then last_time_stamp
        end as map_time_stamp, *
    from (
        select id, package_order, username, warehouse_id, type, time_stamp, type_log
                last_value (if(type_log = 'session', time_stamp, null) ignore nulls) over (partition by username, type, warehouse_id order by time_stamp, type_log, id rows between unbounded preceding and current row) as first_time_stamp,
                last_value (if(type_log = 'session', id,         null) ignore nulls) over (partition by username, type, warehouse_id order by time_stamp, type_log, id rows between unbounded preceding and current row) as first_id,
                first_value(if(type_log = 'session', time_stamp, null) ignore nulls) over (partition by username, type, warehouse_id order by time_stamp, type_log, id rows between current row and unbounded following) as last_time_stamp,
                first_value(if(type_log = 'session', id,         null) ignore nulls) over (partition by username, type, warehouse_id order by time_stamp, type_log, id rows between current row and unbounded following) as last_id
        from map_id_time_union
    ) t where type_log = 'log_package'
) t where abs(unix_timestamp(time_stamp) - unix_timestamp(map_time_stamp)) <= 600 minutes