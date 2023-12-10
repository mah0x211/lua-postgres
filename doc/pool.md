# postgres.pool

defined in [postgres.pool](../lib/pool.lua) module. 


## Usage

```lua
-- create a new instance of postgres.pool.
local pool = require('postgres.pool').new()

-- create a new instance of postgres.pool.connection.
-- connect to the server with the following environment variables:
-- PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD, PGSSLMODE, ...
-- please see the following URL for details:
-- https://www.postgresql.org/docs/current/libpq-envars.html
local conn = pool.get()

-- do something with the connection.
-- ...

-- release the connection to the pool.
pool:release(conn)

-- after using the connection, get the connection from the pool.

-- get a pooled connection.
conn = pool:get()

-- do something with the connection.
-- ...
```


## pool = pool.new( [maxconn [, maxidle [, chkintvl]]] )

create a new instance of `postgres.pool`.

**Parameters**

- `maxconn:number`: maximum number of connections in the pool. if `maxconn` is `nil`, the default value is `0` (unlimited).
- `maxidle:number`: maximum number of idle connections in the pool. if `maxidle` is `nil`, the default value is `0` (disabled).
- `chkintvl:number`: interval of checking idle connections in seconds. if `chkintvl` is `nil`, the default value is `30`.

**Returns**

- `pool:postgres.pool`: instance of `postgres.pool`.


## conn, err, again, timout = pool:get( [conninfo] )

get a `postgres.pool.connection` instance from the pool, or create a new connection.

**Parameters**

- `conninfo:string`: connection information string.

**Returns**

- `conn:postgres.pool.connection?`: instance of `postgres.pool.connection`.
- `err:any`: error message.
- `again:boolean`: if `true`, pool is full.
- `timout:boolean`: if `true`, new connection establishment has timed out.


## ok, err, timout = pool:release( conn [, destroy] )

release a `postgres.pool.connection` instance to the pool.

this method will do the following:

- closes the connection if the pool is closed.
- retrieves the `ReadyForQuery` message from the server before inserting it into the pool.
- removes the oldest idle connection from the pool if number of idle connections is greater than `maxidle`.

**Parameters**

- `conn:postgres.pool.connection`: instance of `postgres.pool.connection`.
- `destroy:boolean`: if `true`, the connection will be closed.

**Returns**

- `ok:boolean`: if `true`, operation succeeded, otherwise `false`.
- `err:any`: error message if failed to retrieve the `ReadyForQuery` message.
- `timout:boolean`: `true` if failed to retrieve the `ReadyForQuery` message due to timeout.


## n, timout = pool:evict( [sec] )

evict idle connections.

**Parameters**

- `sec:number`: time limit in seconds for evicting idle connections. if `sec` is `nil`, the default value is `0` (unlimited).

**Returns**

- `n:number`: number of evicted connections.
- `timout:boolean`: `true` if timed out.
