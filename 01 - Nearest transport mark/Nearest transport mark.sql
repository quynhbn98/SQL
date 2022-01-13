-- query engine: Impala
-- SLQ query

SELECT from_point, to_point, time_stamp - interval T hours as from_time, transport_mark
FROM (
    SELECT *,
        case
            when (prev_type = 'real' or prev_type is null) and (next_type = 'real' or next_type is null) then next_time
            else lead(next_time,1) over (partition by from_point, to_point order by time_stamp, next_time)
            end as transport_mark
    FROM (
        SELECT *
        FROM (
            SELECT from_point, to_point,
                lag (type,1) over (partition by from_point, to_point order by time_stamp) as prev_type,
                type,
                lead(type,1) over (partition by from_point, to_point order by time_stamp) as next_type,
                lag (time_stamp,1) over (partition by from_point, to_point order by time_stamp) as prev_time,
                time_stamp,
                lead(time_stamp,1) over (partition by from_point, to_point order by time_stamp) as next_time
            FROM (
                SELECT distinct from_point, to_point, from_time + interval T hours as time_stamp, 'dummy' as type
                FROM input_table 
                UNION
                SELECT distinct from_point, to_point, to_time as time_stamp, 'real' as type
                FROM input_table
            ) t
        ) tt
    WHERE type = 'dummy' and not ((prev_type = 'dummy' and prev_type is not null) and (next_type = 'dummy' and next_type is not null))
    ) ttt
) tttt
WHERE not (prev_type = 'dummy' and prev_type is not null);