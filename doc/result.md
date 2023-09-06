# postgres.result

defined in [postgres.result](../lib/result.lua) module.


## result = result.new( conn, res )

create a new instance of `postgres.result`.

**Parameters**

- `conn:postgres.connection`: instance of [postgres.connection](connection.md).
- `res:postgres.pgresult`: instance of [postgres.pgresult](pgresult.md).

**Returns**

- `result:postgres.result`: instance of `postgres.result`.


## result, err, timeout = result:next( [sec] )

fetch the next `postgres.result` in the chain.

**Parameters**

- `sec:number`: timeout in seconds.

**Returns**

- `result:postgres.result`: instance of `postgres.result`.
- `err:any`: error message.
- `timeout:boolean`: `true` if the next result fetch times out.


## result:clear()

free the memory used by the `postgres.pgresult` instance.


## ok, err, timeout = result:close()

clear all `postgres.result` instances in the chain.

**Returns**

- `ok:boolean`: `true` if the result is closed, otherwise `false`.
- `err:any`: error message.
- `timeout:boolean`: `true` if the next result fetch times out.


## status = result:status()

get the status of the `postgres.pgresult` instance.

**Returns**

- `status:string`: the `status` field of the statistics table.


## stat = result:stat()

get the statistics of the `postgres.pgresult` instance.

**Returns**

- `stat:table`: statistics of the `postgres.pgresult` instance that contains the followings.
    - `status:string`: return value of the `PQresStatus` function.
    - `cmd_status:string`: return value of the `PQcmdStatus` function.
    - if `status` is `tuples_ok` or `single_tuple`, the following fields are available.
        - `ntuples:number`: return value of the `PQntuples` function.
        - if `ntuples` field is greater than `0`, the following fields are available.
            - `nfields:number`: return value of the `PQnfields` function.
            - `binary_tuples:number`: return value of the `PQbinaryTuples` function.
            - `fields:table`: list of the field.
                - `col:number`: number of the column.
                - `name:string`: return value of the `PQfname` function.
                - `table:number`: return value of the `PQftable` function.
                - `tablecol:number`: return value of the `PQftablecol` function.
                - `type:number`: return value of the `PQftype` function.
                - `size:number`: return value of the `PQfsize` function.
                - `mod:number`: return value of the `PQfmod` function.
                - `format:number`: return value of the `PQfformat` function.
    - if `status` is `command_ok`, `tuples_ok` or `single_tuple`
        - `cmd_tuples:number`: return value of the `PQcmdTuples` function.
        - `oid_value:number`: return value of the `PQoidValue` function.
        - if `PQnparams` > `0`, the following fields are available
            - `nparams:number`: return value of the `PQnparams` function.
            - `params:integer[]`: list of the parameter types.

**Example**
    
```lua
local connection = require('postgres.connection')
local conn = assert(connection.new())
local res = assert(conn:query('SELECT 1'))

-- get the statistics of the `postgres.pgresult` instance.
local dump = require('dump')
print(dump(res:stat()))
-- {
--     binary_tuples = 0,
--     cmd_status = "SELECT 1",
--     cmd_tuples = 1,
--     fields = {
--         -- field can be accessed by column number or field name.
--         [1] = {
--             col = 1,
--             format = 0,
--             mod = -1,
--             name = "?column?",
--             size = 4,
--             table = 0,
--             tablecol = 0,
--             type = 23
--         },
--         ["?column?"] = {
--             col = 1,
--             format = 0,
--             mod = -1,
--             name = "?column?",
--             size = 4,
--             table = 0,
--             tablecol = 0,
--             type = 23
--         }
--     },
--     nfields = 1,
--     ntuples = 1,
--     oid_value = 0,
--     status = "tuples_ok"
-- }
res:close()
```


## val = result:value( row, col )

get the value of the column in the row.

**Parameters**

- `row:number`: row number.
- `col:number`: column number.

**Returns**

- `val:string`: value of the column in the row.


## status, nrow = result:rowinfo()

get the status and the number of the row. this function is available only if the `status` field of the statistics table is `tuples_ok` or `single_tuple`.

**Returns**

- `status:string`: the `status` field of the statistics table.
- `nrow:number`: the number of the row.


## rows, err = result:rows()

get the `postgres.rows` instance. 

**Returns**

- `rows:postgres.rows`: instance of `postgres.rows`.
- `err:any`: error message.

**Example**

```lua
local connection = require('postgres.connection')

-- connect to the database
local conn = assert(connection.new())

-- execute a query
local res = assert(conn:query('SELECT * FROM pg_database;'))

-- get the stat table
local stat = assert(res:stat())

local recwidth = #(tostring(stat.ntuples))
local colwidth = 0
for _, field in ipairs(stat.fields) do
    colwidth = math.max(colwidth, #field.name)
end

-- read the result rows
local nrec = 0
local rows = assert(res:rows())
while rows do
    nrec = nrec + 1
    print(('-[ RECORD %' .. recwidth .. 'd ]-+------------'):format(nrec))

    -- read the columns of the current row
    local field, value = rows:read()
    while field do
        print(('%' .. colwidth .. 's | %s'):format(field.name, value))
        field, value = rows:read()
    end
    -- above displays the following results.
    --     -[ RECORD 1 ]-+------------
    --            oid | 5
    --        datname | postgres
    --         datdba | 10
    --       encoding | 6
    -- datlocprovider | c
    --  datistemplate | f
    --   datallowconn | t
    --   datconnlimit | -1
    --   datfrozenxid | 717
    --     datminmxid | 1
    --  dattablespace | 1663
    --     datcollate | en_US.utf8
    --       datctype | en_US.utf8
    --   daticulocale | nil
    -- datcollversion | 2.36
    --         datacl | nil

    -- read the next row
    if not rows:next() then
        -- no more rows
        break
    end
end

-- close the result
res:close()
```
