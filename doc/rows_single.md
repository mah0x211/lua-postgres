# postgres.rows.single

defined in [postgres.rows.single](../lib/rows/single.lua) module and inherits [postgres.rows](rows.md) module.

`postgres.rows.single` is a class for retrieving query results row by row.


## ok, err, timeout = rows:next( [msec] )

retrieve the next row and reset the column position to the first column.

**Parameters**

- `msec:integer`: timeout in milliseconds.

**Returns**

- `ok:boolean`: `true` on success.
- `err:any`: error message.
- `timeout:boolean`: `true` if timeout.
