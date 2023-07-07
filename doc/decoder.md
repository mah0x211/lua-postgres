# postgres.decoder

defined in [postgres.decoder](../lib/decoder.lua) module.


## decoder = decoder.new()

create a new instance of `postgres.decoder`.

**Returns**

- `decoder:postgres.decoder`: instance of `postgres.decoder`.


## decoder:register_name2dec( name, decodefn )

register a decoder function for the specified name.

**Parameters**

- `name:string`: name of the decoder.
- `decodefn:function`: decode function as follows:
    ```lua
    --- decoder function
    --- @param str string
    --- @return val any
    --- @return err any
    function decodefn( str )
        -- decode str and return the decoded value.
        -- local val, err = ...
        return val, err
    end
    ```

## decoder:register_oid2name( oid, name )

register a name for the specified oid.

**Parameters**

- `oid:integer`: oid of the data type.
- `name:string`: name of the decoder.


## decoder:register( oid, name, decodefn )

register a decoder function for the specified oid and name.

**NOTE:**

this method is equivalent to calling 
`decoder:register_name2dec( name, decodefn )` after `decoder:register_oid2name( oid, name )`.

**Parameters**

- `oid:integer`: oid of the data type.
- `name:string`: name of the decoder.
- `decodefn:function`: decode function.


## val, err = decoder:decode_by_name( name, str )

decode the specified string with the decode function associated with the specified name.

**NOTE**

if the decode function for the specified name is not registered, it just returns the specified string.

**Parameters**

- `name:integer`: name of the data type.
- `str:string`: string to decode.

**Returns**

- `val:any`: decoded value.
- `err:any`: decode error.


## val, err = decoder:decode_by_oid( oid, str )

decode the specified string with the decode function associated with the specified oid.

**NOTE**

if the decode function for the specified oid is not registered, it just returns the specified string.

**Parameters**

- `oid:integer`: oid of the data type.
- `str:string`: string to decode.

**Returns**

- `val:any`: decoded value.
- `err:any`: decode error.

