lua-postgres
====

[![test](https://github.com/mah0x211/lua-postgres/actions/workflows/test.yml/badge.svg)](https://github.com/mah0x211/lua-postgres/actions/workflows/test.yml)
[![codecov](https://codecov.io/gh/mah0x211/lua-postgres/branch/master/graph/badge.svg)](https://codecov.io/gh/mah0x211/lua-postgres)

PostgreSQL client for lua.


## Installation

```
luarocks install postgres
```

***

## Usage

```lua
local connection = require('postgres.connection')

-- connect to the database
local conn = assert(connection.new())

-- execute a query and retrieve the RowDescription message
local msg = assert(conn:query('SELECT * FROM pg_database;'))

-- calculate the width of the columns for printing
local colwidth = 0
for _, field in ipairs(msg.fields) do
    colwidth = math.max(colwidth, #field.name)
end

-- read the result rows
local nrec = 0
local rows = assert(msg:rows())
-- retrieve the DataRow message  
while rows:next() do
    nrec = nrec + 1
    print(('-[ RECORD %d ]-+------------'):format(nrec))

    -- read the columns of the current row
    local field, value = rows:read()
    while field do
        print(('%' .. colwidth .. 's | %s'):format(field.name, value or ''))
        field, value = rows:read()
    end
end

if rows.complete then
    print('----------------------------')
    print(('%s %d rows\n'):format(rows.complete.tag, rows.complete.rows))
end

rows:close()
```


## API Reference

please see [doc/README.md](doc/README.md).


## Not Yet Implemented

- Copy query
- Unix domain socket connection
- SSL connection
- MD5 password authentication
- SCRAM-SHA-256-PLUS authentication
- GSSAPI authentication
- SSPI authentication
- Ident authentication
- Peer authentication
- Certificate authentication


## License

MIT License



