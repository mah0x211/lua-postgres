# postgres.rows

defined in [postgres.rows](../lib/rows.lua) module.


## rows = rows.new( conn, fields )

create a new instance of `postgres.rows`.

**Parameters**

- `conn:postgres.connection`: instance of [postgres.connection](connection.md).
- `fields:table`: the fields property of the [RowDescription](message/row_description.md) message.

**Returns**

- `rows:postgres.rows`: instance of `postgres.rows`.


## ok, err, timeout = rows:close()

retrieve the message from the server until the [CommandComplete](message/command_complete.md) or [ErrorResponse](message/error_response.md) message is received.

**Returns**

- `ok:boolean`: `true` on success.
- `err:any`: error message. this value can be accessed by `rows.error` property.
- `timeout:boolean`: `true` on timeout. this value can be accessed by `rows.is_timeout` property.


## ok, err, timeout = rows:next()

retrieve the next `DataRow` message and reset the column position to the first column.  
if the `DataRow` message is received, it returns `true`, otherwise it returns `false`.

**Returns**

- `ok:boolean`: `true` on success.
- `err:any`: error message. this value can be accessed by `rows.error` property.
- `timeout:boolean`: `true` on timeout. this value can be accessed by `rows.is_timeout` property.

**Usage**

```lua
-- read the next row
local ok, err, timeout = rows:next()
while ok do
    --- do something
    ok, err, timeout = rows:next()
end

if err then
    print(err)
elseif timeout then
    print('timeout')
end
```

you can use `rows.error` and `rows.is_timeout` properties.

```lua
-- read the next row
while rows:next() do
    --- do something
end

-- check the error
if rows.error then
    print(rows.error)
elseif rows.is_timeout then
    print('timeout')
end
```


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
local msg = assert(conn:query([[
    SELECT 1::integer, 'foo', '1999-05-12 12:14:01.1234'::timestamp
]]))
local rows = assert(msg:get_rows())
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
rows:close()

-- above code outputs the following:
-- {
--     field = {
--         col = 1,
--         format = "text",
--         mod = -1,
--         name = "int4",
--         size = 4,
--         table_col = 0,
--         table_oid = 0,
--         type_oid = 23
--     },
--     value = "1"
-- }
-- {
--     field = {
--         col = 2,
--         format = "text",
--         mod = -1,
--         name = "?column?",
--         size = -1,
--         table_col = 0,
--         table_oid = 0,
--         type_oid = 25
--     },
--     value = "foo"
-- }
-- {
--     field = {
--         col = 3,
--         format = "text",
--         mod = -1,
--         name = "timestamp",
--         size = 8,
--         table_col = 0,
--         table_oid = 0,
--         type_oid = 1114
--     },
--     value = "1999-05-12 12:14:01.1234"
-- }
```


## field, val, err = rows:scanat( col [, decoder] )

read the column info and the value at the specified column position then decode the value.

**Parameters**

- `col:integer|string`: column number or name.
- `decoder:postgres.decoder`: [postgres.decoder](decoder.md) object. if not specified, use the default decoder.

**Returns**

- `field:table`: column info.
- `val:string`: value of the column.
- `err:any`: decode error.


## field, val, err = rows:scan( [decoder] )

read the column info and the value at the current column position then decode the value.  
after reading, the current position is moved to the next column.

**Parameters**

- `decoder:postgres.decoder`: `postgres.decoder` object. if not specified, use the default decoder.

**Returns**

- `field:table`: column info.
- `val:string`: value of the column.
- `err:any`: decode error.

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
local rows = assert(res:get_rows())
while rows:next() do
    -- read the columns of the current row and update the current position
    local field, value, err = rows:scan()
    while field do
        -- dump the column info and the value
        print(dump({
            field = field,
            value = value,
        }))
        field, value, err = rows:scan()
    end
    if err then
        error(err)
    end
end

-- close the result
rows:close()

-- above code outputs the following:
-- {
--     field = {
--         col = 1,
--         format = "text",
--         mod = -1,
--         name = "int4",
--         size = 4,
--         table_col = 0,
--         table_oid = 0,
--         type_oid = 23
--     },
--     value = 1
-- }
-- {
--     field = {
--         col = 2,
--         format = "text",
--         mod = -1,
--         name = "?column?",
--         size = -1,
--         table_col = 0,
--         table_oid = 0,
--         type_oid = 25
--     },
--     value = "foo"
-- }
-- {
--     field = {
--         col = 3,
--         format = "text",
--         mod = -1,
--         name = "timestamp",
--         size = 8,
--         table_col = 0,
--         table_oid = 0,
--         type_oid = 1114
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
