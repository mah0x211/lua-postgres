# postgres.canceler

defined in [postgres.canceler](../lib/canceler.lua) module.


## cancel = canceler.new( conninfo, pid, key )

create a new `postgres.canceler` object.

**Parameters**

- `conninfo:string`: connection uri string. see [libpq documentation: 34.1.1. Connection Strings](https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING-URIS) for details. if not specified, [libpq documentation: 34.15. Environment Variables](https://www.postgresql.org/docs/current/libpq-envars.html) is used.
- `pid:integer`: the process ID (PID) of the backend server process.
- `key:integer`: the cancel key of the backend server process.

**Returns**

- `cancel:postgres.canceler`: the `postgres.canceler` object.
- `err:any`: error message.


## ok, err, timeout = canceler:cancel()

send a `CancelRequest` message to the server.

**Returns**

- `ok:boolean`: `true` on success.
- `err:any`: error message.
- `timeout:boolean`: `true` if timeout.

