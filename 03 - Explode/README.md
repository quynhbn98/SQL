In this situation, I can only use Impala as the query engine, and it doesn't have `explode` function 

But I need to turn data like this

|config_id| src_id_list| dst_id_list|
|---|---|---|
|1  | [1,2,3]  | [4,5,6,7]

into this

|config_id| src_id_list| dst_id_list| src_name |dst_name | 
|---|---|---|---|---|
|1  | [1,2,3]  | [4,5,6,7] | name1, name2, name3 | name4, name5, name6, name7

basically translate data for IT into data for business users

so I have to go around :)

Its performance is not good (query time is long), but, I get what my boss needs :))