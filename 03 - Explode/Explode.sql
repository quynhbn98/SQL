-- src stands for source, dst for destination

with src_list as (
	select src.config_id, src.src_id_list, src.group_concat(name.name) src_name
	from (
		select config_id, src_id_list, split_count, split_part(src_id, ',', num.rn) as src_id
		from (
			select config_id, src_id_list,
				btrim(src_id_list,'[]"') src_id,
				length(src_id_list) - length(replace(src_id_list, ',' , '')) + 1 as split_count
			from config_table
		) src
		join (select row_number() over (order by created) rn from name_table ) num on num.rn <= split_count
	) src
	join name_table name on cast(src.src_id as int) = name.id
	group by 1,2
),

with dst_list as (
	select dst.config_id, dst.dst_id_list, dst.group_concat(name.name) dst_name
	from (
		select config_id, dst_id_list, split_count, split_part(dst_id, ',', num.rn) as dst_id
		from (
			select config_id, dst_id_list,
				btrim(dst_id_list,'[]"') dst_id,
				length(dst_id_list) - length(replace(dst_id_list, ',' , '')) + 1 as split_count
			from config_table
		) dst
		join (select row_number() over (order by created) rn from name_table ) num on num.rn <= split_count
	) dst
	join name_table name on cast(dst.dst_id as int) = name.id
	group by 1,2

select a.config_id, a.src_id_list, b.dst_id_list, a.src_name, b.dst_name
from src_list a
join dst_list b on a.config_id = b.config_id
