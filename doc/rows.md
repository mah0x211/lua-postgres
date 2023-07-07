# postgres.rows

defined in [postgres.rows](../lib/rows.lua) module.


## rows = rows.new( res, nrow )

create a new instance of `postgres.rows`.

**Parameters**

- `res:postgres.result`: instance of [postgres.result](result.md).
- `nrow:number`: number of the row.

**Returns**

- `rows:postgres.rows`: instance of `postgres.rows`.


## ok, err, timeout = rows:close()

close the `postgres.result` instance.

see also documentation of [postgres.result:close()](result.md#ok-err-timeout--resultclose) method.


## ok = rows:next()

move the current position to the next row and reset the column position to the first column.  

**NOTE**

you should call this method before reading the first row.

**Returns**

- `ok:boolean`: `true` if the current position is moved to the next row.


## result = rows:result()

get the `postgres.result` instance.

**Returns**

- `result:postgres.result`: instance of `postgres.result`.


## field, val = rows:readat( col )

read the column info and the value at the specified column position.

**Parameters**

- `col:integer|string`: column number or name.

**Returns**

- `field:table`: column info.
- `val:string`: value of the column.


## field, val = rows:read()

read the column info and the value at the current column position.  
after reading, the current position is moved to the next column.

**Returns**

- `field:table`: column info.
- `val:string`: value of the column.

**Example**

```lua
local dump = require('dump')
local connection = require('postgres.connection')

-- connect to the database
local conn = assert(connection.new())

-- execute a query
local res = assert(conn:query([[
    SELECT 1::integer, 'foo', '1999-05-12 12:14:01.1234'::timestamp
]]))
local rows = assert(res:rows())
while rows:next() do
    -- read the columns of the current row and update the current position
    local field, value = rows:read()
    while field do
        -- dump the column info and the value
        print(dump({
            field = field,
            value = value,
        }))
        field, value = rows:read()
    end
end
-- close the result
res:close()

-- above code outputs the following:
-- {
--     field = {
--         col = 1,
--         format = 0,
--         mod = -1,
--         name = "int4",
--         size = 4,
--         table = 0,
--         tablecol = 0,
--         type = 23
--     },
--     value = "1"
-- }
-- {
--     field = {
--         col = 2,
--         format = 0,
--         mod = -1,
--         name = "?column?",
--         size = -1,
--         table = 0,
--         tablecol = 0,
--         type = 25
--     },
--     value = "foo"
-- }
-- {
--     field = {
--         col = 3,
--         format = 0,
--         mod = -1,
--         name = "timestamp",
--         size = 8,
--         table = 0,
--         tablecol = 0,
--         type = 1114
--     },
--     value = "1999-05-12 12:14:01.1234"
-- }
```


## val, err, field = rows:scanat( col [, decoder] )

read the column info and the value at the specified column position then decode the value.

**Parameters**

- `col:integer|string`: column number or name.
- `decoder:postgres.decoder`: [postgres.decoder](decoder.md) object. if not specified, use the default decoder.

**Returns**

- `val:string`: value of the column.
- `err:any`: decode error.
- `field:table`: column info.


## val, err, field = rows:scan( [decoder] )

read the column info and the value at the current column position then decode the value.  
after reading, the current position is moved to the next column.

**Parameters**

- `decoder:postgres.decoder`: `postgres.decoder` object. if not specified, use the default decoder.

**Returns**

- `val:string`: value of the column.
- `err:any`: decode error.
- `field:table`: column info.

**Example**

```lua
local dump = require('dump')
local connection = require('postgres.connection')

-- connect to the database
local conn = assert(connection.new())

-- execute a query
local res = assert(conn:query([[
    SELECT 1::integer, 'foo', '1999-05-12 12:14:01.1234'::timestamp
]]))
local rows = assert(res:rows())
while rows:next() do
    -- read the columns of the current row and update the current position
    local value, err, field = rows:scan()
    while value do
        -- dump the column info and the value
        print(dump({
            field = field,
            value = value,
        }))
        value, err, field = rows:scan()
    end
    if err then
        error(err)
    end
end

-- close the result
res:close()

-- above code outputs the following:
-- {
--     field = {
--         col = 1,
--         format = 0,
--         mod = -1,
--         name = "int4",
--         size = 4,
--         table = 0,
--         tablecol = 0,
--         type = 23
--     },
--     value = 1
-- }
-- {
--     field = {
--         col = 2,
--         format = 0,
--         mod = -1,
--         name = "?column?",
--         size = -1,
--         table = 0,
--         tablecol = 0,
--         type = 25
--     },
--     value = "foo"
-- }
-- {
--     field = {
--         col = 3,
--         format = 0,
--         mod = -1,
--         name = "timestamp",
--         size = 8,
--         table = 0,
--         tablecol = 0,
--         type = 1114
--     },
--     value = {
--         day = 12,
--         hour = 12,
--         min = 14,
--         month = 5,
--         sec = 1,
--         usec = 123400,
--         year = 1999
--     }
-- }
```
