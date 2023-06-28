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


