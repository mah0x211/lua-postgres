# postgres.cancel

defined in [postgres.cancel](../lib/cancel.lua) module.


## cancel = cancel.new( host, port, pid, key )

create a new `postgres.cancel` object.

**Parameters**

- `host:string`: the host name or IP address of the server.
- `port:string|integer`: the port number of the server.
- `pid:integer`: the process ID (PID) of the backend server process.
- `key:integer`: the cancel key of the backend server process.

**Returns**

- `cancel:postgres.cancel`: the `postgres.cancel` object.


## ok, err, timeout = cancel:cancel( [sec] )

send a cancel request to the server.

**Parameters**

- `sec:number`: the maximum time to wait for conneting to the server in seconds.

**Returns**

- `ok:boolean`: `true` on success.
- `err:any`: error message.
- `timeout:boolean`: `true` if timeout.

