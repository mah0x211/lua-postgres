# postgres.rows.single

defined in [postgres.rows.single](../lib/rows/single.lua) module and inherits [postgres.rows](rows.md) module.

`postgres.rows.single` is a class for retrieving query results row by row.


## ok, err, timeout = rows:next( [sec] )

retrieve the next row and reset the column position to the first column.

**Parameters**

- `sec:number`: timeout in seconds.

**Returns**

- `ok:boolean`: `true` on success.
- `err:any`: error message. this value can be accessed by `rows.error` property.
- `timeout:boolean`: `true` on timeout. this value can be accessed by `rows.is_timeout` property.

**Usage**

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
