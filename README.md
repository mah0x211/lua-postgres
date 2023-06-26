lua-postgres
====

[![test](https://github.com/mah0x211/lua-postgres/actions/workflows/test.yml/badge.svg)](https://github.com/mah0x211/lua-postgres/actions/workflows/test.yml)
[![codecov](https://codecov.io/gh/mah0x211/lua-postgres/branch/master/graph/badge.svg)](https://codecov.io/gh/mah0x211/lua-postgres)

PostgreSQL client for lua.

**NOTE: this module is under heavy development.**

***

## Installation

```
luarocks install postgres
```

## Usage

```lua
local connection = require('postgres.connection')

-- connect to the database
local conn = assert(connection.new())

-- execute a query
local res = assert(conn:query('SELECT * FROM pg_database;'))

-- get the stat table
local stat = assert(res:stat())

-- calculate the width of the rows and columns for printing
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

    -- read the next row
    if not rows:next() then
        -- no more rows
        break
    end
end

-- close the result
res:close()
```


## API Reference

please see [doc/README.md](doc/README.md).

