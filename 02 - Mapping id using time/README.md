### I. Data input

- Each time when a warehouse staff get a batch of packages from truck down into warehouse, or vice verse
    - a working session is created --> insert new record in `sessions_table`
     
    |id | package_order| username| warehouse_id| type| created|
    |---| ---| ---| ---| ---| ---|
    |1 | X | someone1 | A | `up`   | timestampt|
    |2 | X | someone2 | B | `down` | timestampt|
    |3 | X | someone3 | B | `up`   | timestampt|
    |4 | X | someone4 | C | `down` | timestampt|
  
    - insert new records in `package_logs_table`

    |id | username| warehouse_id| type| created| list_bags |
    |---| ---| ---| ---| ---| ---|
    |1 | someone1 | A | `up`   | timestampt| [1,2,3]
    |2 | someone2 | B | `down` | timestampt| [5,6,7]
    |3 | someone3 | B | `up`   | timestampt| [8,9]
    |4 | someone4 | C | `down` | timestampt| [10,11,12]
- 2 actions inserting in 2 tables don't happen in the same transaction after the api is called: it inserts in `sessions_table` right away and records for `package_logs_table` waits in queue, so there're 3 scenarios:
    - `sessions_table.created = package_logs_table.created` : ideal case
    - `sessions_table.created < package_logs_table.created` : normal case, record in queue written after 
    - `sessions_table.created = package_logs_table.created` : rare case, `sessions_table` is commited even slower than queue 
- Both tables have:
    - `username`: who executed the action
    - `type`: `up` for get packages from warehouses to trucks, `down` for the other way
    - `warehouse_id`: where the action took place
- There are 2 seperate apis for `up` and `down`, and 2 separate queues
- Which records get in queue first will be written first

### II. Data output
- Map each id of `package_logs_table` with an id of `sessions_table`

### III. Steps
- Step 1: select id, created, warehouse_id, username, type from 2 tables and union
- Step 2: using window function `partition by warehouse_id, username, type order by created` to find the last `sessions_table.id` before and the first `sessions_table.id` after a `package_logs_table.id`
- Step 3: between the first and last value from step 2, select which has minimum time difference compared to `package_logs_table.created`

|type_time      |id     |last session_id before |first session_id after|map session_id |
|---|---|---|---|---|
|session        |A      |-                      |-                     |-
|log_package    |-      |A (1)                  |B (2)                 |A
|log_package    |-      |A (2)                  |B (2)                 |A
|session        |B      |-                      |-                     |-
|session        |C      |-                      |-                     |-
|log_package    |-      |C (5)                  |D (2)                 |D
|session        |D      |-                      |-                     |-
