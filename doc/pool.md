# postgres.pool

defined in [postgres.pool](../lib/pool.lua) module. 


## Usage

```lua
-- create a new instance of postgres.pool.
local pool = require('postgres.pool').new()

-- create a new instance of postgres.connection.
-- connect to the server with the following environment variables:
-- PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD, PGSSLMODE, ...
-- please see the following URL for details:
-- https://www.postgresql.org/docs/current/libpq-envars.html
local conn = require('postgres.connection').new()

-- get the connection information string.
local conninfo = conn:conninfo()

-- do something with the connection.
-- ...

-- set the connection to the pool.
pool:set(conn)

-- after using the connection, get the connection from the pool.

-- get a pooled connection.
conn = pool:get(conninfo)

-- do something with the connection.
-- ...
```


## pool = pool.new()

create a new instance of `postgres.pool`.

**Returns**

- `pool:postgres.pool`: instance of `postgres.pool`.


## pool:set( conn )

set a `postgres.connection` instance to the pool.

**Parameters**

- `conn:postgres.connection`: instance of `postgres.connection`.


## conn = pool:get( conninfo )

get a `postgres.connection` instance associated with the specified connection information from the pool.

**Parameters**

- `conninfo:string`: connection information string.

**Returns**

- `conn:postgres.connection?`: instance of `postgres.connection`.

**NOTE**

you must confirm that the connection is still live before using it.

```lua
-- get a pooled connection.
local conn = pool:get(conninfo)
while conn do
    -- send a ping query to the server
    local res = conn:query('SELECT 1;')
    if res then
        -- the connection is valid.
        res:close()
        break
    end

    -- the connection is invalid.
    conn:close()
    -- get a next pooled connection.
    conn = pool:get(conninfo)
end

-- no pooled connection.
if not conn then
    -- create a new connection.
    conn = postgres.connect(conninfo)
end

-- do something with the connection.
-- ...
```


## n, err = pool:clear( [callback], [n] )

close all connections in the pool and clear the pool.

if `callback` returns `false`, the connection is not closed and immediately returns a number of closed connections and an error message.

**Parameters**

- `callback:function`: callback function as follows:
    ```lua
    --- callback called when a connection before closing.
    --- @param conninfo string connection information string.
    --- @param conn postgres.connection instance of postgres.connection.
    --- @return boolean true if the connection can be closed, otherwise false.
    --- @return string error message if the connection can't be closed.
    function callback(conninfo, conn)
        return true
    end
    ```
- `n:number`: number of connections to close. if `n` is `nil`, all connections are closed.

**Returns**

- `n:number`: number of closed connections.
- `err:string`: error message.
