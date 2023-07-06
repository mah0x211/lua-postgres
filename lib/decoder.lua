--
-- Copyright (C) 2023 Masatoshi Fukunaga
--
-- Permission is hereby granted, free of charge, to any person obtaining a copy
-- of this software and associated documentation files (the "Software"), to deal
-- in the Software without restriction, including without limitation the rights
-- to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
-- copies of the Software, and to permit persons to whom the Software is
-- furnished to do so, subject to the following conditions:
--
-- The above copyright notice and this permission notice shall be included in
-- all copies or substantial portions of the Software.
--
-- THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
-- IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
-- FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.  IN NO EVENT SHALL THE
-- AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
-- LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
-- OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
-- THE SOFTWARE.
--
-- composite
--- @alias itemfn fun(s:string, is_quoted:boolean, ctx:any):(v:any, err:any)
--- @alias decodefn fun(s:string, fn:itemfn, ctx:any):(v:any, err:any)
--- @type fun(s:string, fn:itemfn, ctx?:any, delim?:string):(v:table, err:any)
local decode_array = require('postgres.decode.array')
--- @type fun(s: string, fn:itemfn, ctx:any):(v:table, err:any)
local decode_range = require('postgres.decode.range')
--- @type fun(s: string, fn:itemfn, ctx:any):(v:table[], err:any)
local decode_multirange = require('postgres.decode.multirange')

-- assign to local
local gsub = string.gsub

--- decode_container_item
--- @param str string
--- @param is_quoted boolean
--- @param decode_itemfn fun(s:string):(v:any, err:any)
--- @return any v
local function decode_container_item(str, is_quoted, decode_itemfn)
    if is_quoted then
        -- remove quotes in first and last position, and unescape quotes
        str = gsub(gsub(str, "^\"(.*)\"$", "%1"), '\\"', '"')
    end
    return decode_itemfn(str)
end

--- decode_container
--- @param s string
--- @param decodefn decodefn
--- @param decode_itemfn fun(s:string):(v:any, err:any)
--- @return integer[][] v
--- @return any err
local function decode_container(s, decodefn, decode_itemfn)
    return decodefn(s, decode_container_item, decode_itemfn)
end

-- primitive
--- @type fun(s: string):(v:boolean, err:any)
local decode_boolean = require('postgres.decode.bool')

--- decode_boolean_array
--- @param s string
--- @return boolean[] v
--- @return any err
local function decode_boolean_array(s)
    return decode_array(s, decode_boolean)
end

--- @type fun(s: string):(v:integer, err:any)
local decode_int = require('postgres.decode.int')

--- decode_int_array
--- @param s string
--- @return integer[] v
--- @return any err
local function decode_int_array(s)
    return decode_array(s, decode_int)
end

--- decode_intrange
--- @param s string
--- @return integer[] v
--- @return any err
local function decode_intrange(s)
    return decode_range(s, decode_int)
end

--- decode_intmrange
--- @param s string
--- @return integer[][] v
--- @return any err
local function decode_intmrange(s)
    return decode_multirange(s, decode_int)
end

--- decode_intrange_array
--- @param s string
--- @return integer[][] v
--- @return any err
local function decode_intrange_array(s)
    return decode_container(s, decode_array, decode_intrange)
end

--- decode_intmrange_array
--- @param s string
--- @return integer[][] v
--- @return any err
local function decode_intmrange_array(s)
    return decode_container(s, decode_array, decode_intmrange)
end

--- @type fun(s: string):(v:number, err:any)
local decode_float = require('postgres.decode.float')

--- decode_float_array
--- @param s string
--- @return number[] v
--- @return any err
local function decode_float_array(s)
    return decode_array(s, decode_float)
end

--- decode_floatrange
--- @param s string
--- @return number[] v
--- @return any err
local function decode_floatrange(s)
    return decode_range(s, decode_float)
end

--- decode_floatmrange
--- @param s string
--- @return number[][] v
--- @return any err
local function decode_floatmrange(s)
    return decode_multirange(s, decode_float)
end

--- decode_floatrange_array
--- @param s string
--- @return number[][] v
--- @return any err
local function decode_floatrange_array(s)
    return decode_container(s, decode_array, decode_floatrange)
end

--- decode_floatmrange_array
--- @param s string
--- @return number[][] v
--- @return any err
local function decode_floatmrange_array(s)
    return decode_container(s, decode_array, decode_floatmrange)
end

-- date/time
--- @type fun(s: string):(v:table, err:any)
local decode_date = require('postgres.decode.date')

--- decode_date_array
--- @param s string
--- @return table[] v
--- @return any err
local function decode_date_array(s)
    return decode_array(s, decode_date)
end

--- decode_daterange
--- @param s string
--- @return table[] v
--- @return any err
local function decode_daterange(s)
    return decode_range(s, decode_date)
end

--- decode_datemrange
--- @param s string
--- @return table[][] v
--- @return any err
local function decode_datemrange(s)
    return decode_multirange(s, decode_date)
end

--- decode_daterange_array
--- @param s string
--- @return table[][] v
--- @return any err
local function decode_daterange_array(s)
    return decode_container(s, decode_array, decode_daterange)
end

--- decode_datemrange_array
--- @param s string
--- @return table[][] v
--- @return any err
local function decode_datemrange_array(s)
    return decode_container(s, decode_array, decode_datemrange)
end

--- @type fun(s: string):(v:table, err:any)
local decode_time = require('postgres.decode.time')

--- decode_time_array
--- @param s string
--- @return table[] v
--- @return any err
local function decode_time_array(s)
    return decode_array(s, decode_time)
end

--- @type fun(s: string):(v:table, err:any)
local decode_timestamp = require('postgres.decode.timestamp')

--- decode_timestamp_array
--- @param s string
--- @return table[] v
--- @return any err
local function decode_timestamp_array(s)
    return decode_container(s, decode_array, decode_timestamp)
end

--- decode_tsrange
--- @param s string
--- @return table[] v
--- @return any err
local function decode_tsrange(s)
    return decode_container(s, decode_range, decode_timestamp)
end

--- decode_tsmrange
--- @param s string
--- @return table[][] v
--- @return any err
local function decode_tsmrange(s)
    return decode_container(s, decode_multirange, decode_timestamp)
end

--- decode_tsrange_array
--- @param s string
--- @return table[][] v
--- @return any err
local function decode_tsrange_array(s)
    return decode_container(s, decode_array, decode_tsrange)
end

--- decode_tsmrange_array
--- @param s string
--- @return table[][] v
--- @return any err
local function decode_tsmrange_array(s)
    return decode_container(s, decode_array, decode_tsmrange)
end

-- binary
--- @type fun(s: string):(v:string, err:any)
local decode_bytea = require('postgres.decode.bytea')

--- decode_bytea_array
--- @param s string
--- @return string[] val
--- @return any err
local function decode_bytea_array(s)
    return decode_array(s, function(str, is_quoted)
        if is_quoted then
            -- remove quotes in first and last position, and unescape escaped backslashes
            str = gsub(gsub(str, "^\"(.*)\"$", "%1"), '\\\\', '\\')
        end
        return decode_bytea(str)
    end)
end

--- @type fun(s: string):(v:string, err:any)
local decode_bit = require('postgres.decode.bit')

--- decode_bit_array
--- @param s string
--- @return string[] val
--- @return any err
local function decode_bit_array(s)
    return decode_array(s, decode_bit)
end

-- search text
--- @type fun(s: string):(v:table[], err:any)
local decode_tsvector = require('postgres.decode.tsvector')

--- decode_tsvector_array
--- @param s string
--- @return table[][] val
--- @return any err
local function decode_tsvector_array(s)
    return decode_container(s, decode_array, decode_tsvector)
end

-- geom
--- @type fun(s: string):(v:number[], err:any)
local decode_point = require('postgres.decode.point')

--- decode_point_array
--- @param s string
--- @return number[][] val
--- @return any err
local function decode_point_array(s)
    return decode_container(s, decode_array, decode_point)
end

--- @type fun(s: string):(v:number[], err:any)
local decode_line = require('postgres.decode.line')

--- decode_line_array
--- @param s string
--- @return number[][] val
--- @return any err
local function decode_line_array(s)
    return decode_container(s, decode_array, decode_line)
end

--- @type fun(s: string):(v:number[][], err:any)
local decode_lseg = require('postgres.decode.lseg')

--- decode_lseg_array
--- @param s string
--- @return number[][] val
--- @return any err
local function decode_lseg_array(s)
    return decode_container(s, decode_array, decode_lseg)
end

--- @type fun(s: string):(v:number[][], err:any)
local decode_box = require('postgres.decode.box')

--- decode_box_array
--- @param s string
--- @return number[][] val
--- @return any err
local function decode_box_array(s)
    return decode_array(s, decode_box, nil, ';')
end

--- @type fun(s: string):(v:number[][], err:any)
local decode_path = require('postgres.decode.path')

--- decode_path_array
--- @param s string
--- @return number[][] val
--- @return any err
local function decode_path_array(s)
    return decode_container(s, decode_array, decode_path)
end

--- @type fun(s: string):(v:number[][], err:any)
local decode_polygon = require('postgres.decode.polygon')

--- decode_polygon_array
--- @param s string
--- @return number[][] val
--- @return any err
local function decode_polygon_array(s)
    return decode_container(s, decode_array, decode_polygon)
end

--- @type fun(s: string):(v:number[], err:any)
local decode_circle = require('postgres.decode.circle')

--- decode_circle_array
--- @param s string
--- @return number[][] val
--- @return any err
local function decode_circle_array(s)
    return decode_container(s, decode_array, decode_circle)
end

--- decode_text
--- @param s string
--- @return string
local function decode_text(s)
    return s
end

--- decode_text_array
--- @param s string
--- @return string[] val
local function decode_text_array(s)
    return decode_container(s, decode_array, decode_text)
end

local decode_char = decode_text
local decode_name = decode_text

local decode_bigint = decode_int
local decode_smallint = decode_int
local decode_integer = decode_int
local decode_oid = decode_int
local decode_tid = decode_int
local decode_xid = decode_int
local decode_cid = decode_int

--- @type fun(s: string):(v:any, err:any)
local decode_json = require('yyjson').decode

--- decode_json_array
--- @param s string
--- @return any[] val
--- @return any err
local function decode_json_array(s)
    return decode_container(s, decode_array, decode_json)
end

local decode_xml = decode_text -- TODO
local decode_xml_array = decode_text_array

local decode_xid8 = decode_xid
local decode_xid8_array = decode_int_array

local decode_cidr = decode_text -- TODO
local decode_cidr_array = decode_text_array

local decode_real = decode_float
local decode_double_precision = decode_float

local decode_macaddr8 = decode_text -- TODO
local decode_macaddr8_array = decode_text_array

local decode_money = decode_float
local decode_money_array = decode_float_array

local decode_char_array = decode_text_array
local decode_name_array = decode_text_array

local decode_smallint_array = decode_int_array

local decode_integer_array = decode_int_array
local decode_tid_array = decode_int_array
local decode_xid_array = decode_int_array
local decode_cid_array = decode_int_array

local decode_character = decode_text
local decode_character_array = decode_text_array

local decode_character_varying = decode_text
local decode_character_varying_array = decode_text_array

local decode_bigint_array = decode_int_array

local decode_real_array = decode_float_array
local decode_double_precision_array = decode_float_array

local decode_oid_array = decode_int_array

local decode_aclitem = decode_text -- TODO
local decode_aclitem_array = decode_text_array

local decode_macaddr = decode_text -- TODO
local decode_macaddr_array = decode_text_array
local decode_inet = decode_text -- TODO
local decode_inet_array = decode_text_array

local decode_time_without_time_zone = decode_time
local decode_timestamp_without_time_zone = decode_timestamp
local decode_timestamp_without_time_zone_array = decode_timestamp_array
local decode_time_without_time_zone_array = decode_time_array
local decode_timestamp_with_time_zone = decode_timestamp
local decode_timestamp_with_time_zone_array = decode_timestamp_array

local decode_interval = decode_text -- TODO
local decode_interval_array = decode_text_array

local decode_numeric_array = decode_float_array

local decode_time_with_time_zone = decode_time
local decode_time_with_time_zone_array = decode_time_array

local decode_bit_varying = decode_bit
local decode_bit_varying_array = decode_bit_array

local decode_numeric = decode_float

local decode_refcursor = decode_text
local decode_refcursor_array = decode_text_array

local decode_uuid = decode_text -- TODO
local decode_uuid_array = decode_text_array

local decode_tsquery = decode_text -- TODO
local decode_tsquery_array = decode_text_array

local decode_jsonb = decode_json
local decode_jsonb_array = decode_json_array

local decode_int4range = decode_intrange
local decode_int4range_array = decode_intrange_array

local decode_numrange = decode_floatrange
local decode_numrange_array = decode_floatrange_array

local decode_tstzrange = decode_tsrange
local decode_tstzrange_array = decode_tsrange_array

local decode_int8range = decode_intrange
local decode_int8range_array = decode_intrange_array

local decode_jsonpath = decode_text -- TODO
local decode_jsonpath_array = decode_text_array

local decode_int4multirange = decode_intmrange
local decode_nummultirange = decode_floatmrange

local decode_tsmultirange = decode_tsmrange
local decode_tstzmultirange = decode_tsmrange
local decode_datemultirange = decode_datemrange

local decode_int8multirange = decode_intmrange
local decode_int4multirange_array = decode_intmrange_array
local decode_nummultirange_array = decode_floatmrange_array

local decode_tsmultirange_array = decode_tsmrange_array
local decode_tstzmultirange_array = decode_tsmrange_array
local decode_datemultirange_array = decode_datemrange_array

local decode_int8multirange_array = decode_intmrange_array

-- default oid to type name mapping table
local OID2NAME = {}
-- default type name to decode function mapping table
local NAME2DEC = {}

--           oid: 16
--          name: boolean
--          type: base
--     type_code: b
--     array_oid: 1000
--      category: boolean
-- category_code: B
--   description: boolean, 'true'/'false'
OID2NAME[16] = "boolean"
NAME2DEC["boolean"] = decode_boolean

--           oid: 17
--          name: bytea
--          type: base
--     type_code: b
--     array_oid: 1001
--      category: user_defined
-- category_code: U
--   description: variable-length string, binary values escaped
OID2NAME[17] = "bytea"
NAME2DEC["bytea"] = decode_bytea

--           oid: 18
--          name: char
--          type: base
--     type_code: b
--     array_oid: 1002
--      category: internal
-- category_code: Z
--   description: single character
OID2NAME[18] = "char"
NAME2DEC["char"] = decode_char

--           oid: 19
--          name: name
--          type: base
--     type_code: b
--     array_oid: 1003
--      category: string
-- category_code: S
--   description: 63-byte type for storing system identifiers
OID2NAME[19] = "name"
NAME2DEC["name"] = decode_name

--           oid: 20
--          name: bigint
--          type: base
--     type_code: b
--     array_oid: 1016
--      category: numeric
-- category_code: N
--   description: ~18 digit integer, 8-byte storage
OID2NAME[20] = "bigint"
NAME2DEC["bigint"] = decode_bigint

--           oid: 21
--          name: smallint
--          type: base
--     type_code: b
--     array_oid: 1005
--      category: numeric
-- category_code: N
--   description: -32 thousand to 32 thousand, 2-byte storage
OID2NAME[21] = "smallint"
NAME2DEC["smallint"] = decode_smallint

--           oid: 23
--          name: integer
--          type: base
--     type_code: b
--     array_oid: 1007
--      category: numeric
-- category_code: N
--   description: -2 billion to 2 billion integer, 4-byte storage
OID2NAME[23] = "integer"
NAME2DEC["integer"] = decode_integer

--           oid: 25
--          name: text
--          type: base
--     type_code: b
--     array_oid: 1009
--      category: string
-- category_code: S
--   description: variable-length string, no limit specified
OID2NAME[25] = "text"
NAME2DEC["text"] = decode_text

--           oid: 26
--          name: oid
--          type: base
--     type_code: b
--     array_oid: 1028
--      category: numeric
-- category_code: N
--   description: object identifier(oid), maximum 4 billion
OID2NAME[26] = "oid"
NAME2DEC["oid"] = decode_oid

--           oid: 27
--          name: tid
--          type: base
--     type_code: b
--     array_oid: 1010
--      category: user_defined
-- category_code: U
--   description: (block, offset), physical location of tuple
OID2NAME[27] = "tid"
NAME2DEC["tid"] = decode_tid

--           oid: 28
--          name: xid
--          type: base
--     type_code: b
--     array_oid: 1011
--      category: user_defined
-- category_code: U
--   description: transaction id
OID2NAME[28] = "xid"
NAME2DEC["xid"] = decode_xid

--           oid: 29
--          name: cid
--          type: base
--     type_code: b
--     array_oid: 1012
--      category: user_defined
-- category_code: U
--   description: command identifier type, sequence in transaction id
OID2NAME[29] = "cid"
NAME2DEC["cid"] = decode_cid

--           oid: 114
--          name: json
--          type: base
--     type_code: b
--     array_oid: 199
--      category: user_defined
-- category_code: U
--   description: JSON stored as text
OID2NAME[114] = "json"
NAME2DEC["json"] = decode_json

--           oid: 142
--          name: xml
--          type: base
--     type_code: b
--     array_oid: 143
--      category: user_defined
-- category_code: U
--   description: XML content
OID2NAME[142] = "xml"
NAME2DEC["xml"] = decode_xml

--           oid: 143
--          name: xml[]
--          type: base
--     type_code: b
--      category: array
-- category_code: A
OID2NAME[143] = "xml[]"
NAME2DEC["xml[]"] = decode_xml_array

--           oid: 199
--          name: json[]
--          type: base
--     type_code: b
--      category: array
-- category_code: A
OID2NAME[199] = "json[]"
NAME2DEC["json[]"] = decode_json_array

--           oid: 271
--          name: xid8[]
--          type: base
--     type_code: b
--      category: array
-- category_code: A
OID2NAME[271] = "xid8[]"
NAME2DEC["xid8[]"] = decode_xid8_array

--           oid: 600
--          name: point
--          type: base
--     type_code: b
--     array_oid: 1017
--      category: geometric
-- category_code: G
--   description: geometric point '(x, y)'
OID2NAME[600] = "point"
NAME2DEC["point"] = decode_point

--           oid: 601
--          name: lseg
--          type: base
--     type_code: b
--     array_oid: 1018
--      category: geometric
-- category_code: G
--   description: geometric line segment '(pt1,pt2)'
OID2NAME[601] = "lseg"
NAME2DEC["lseg"] = decode_lseg

--           oid: 602
--          name: path
--          type: base
--     type_code: b
--     array_oid: 1019
--      category: geometric
-- category_code: G
--   description: geometric path '(pt1,...)'
OID2NAME[602] = "path"
NAME2DEC["path"] = decode_path

--           oid: 603
--          name: box
--          type: base
--     type_code: b
--     array_oid: 1020
--      category: geometric
-- category_code: G
--   description: geometric box '(lower left,upper right)'
OID2NAME[603] = "box"
NAME2DEC["box"] = decode_box

--           oid: 604
--          name: polygon
--          type: base
--     type_code: b
--     array_oid: 1027
--      category: geometric
-- category_code: G
--   description: geometric polygon '(pt1,...)'
OID2NAME[604] = "polygon"
NAME2DEC["polygon"] = decode_polygon

--           oid: 628
--          name: line
--          type: base
--     type_code: b
--     array_oid: 629
--      category: geometric
-- category_code: G
--   description: geometric line
OID2NAME[628] = "line"
NAME2DEC["line"] = decode_line

--           oid: 629
--          name: line[]
--          type: base
--     type_code: b
--      category: array
-- category_code: A
OID2NAME[629] = "line[]"
NAME2DEC["line[]"] = decode_line_array

--           oid: 650
--          name: cidr
--          type: base
--     type_code: b
--     array_oid: 651
--      category: net
-- category_code: I
--   description: network IP address/netmask, network address
OID2NAME[650] = "cidr"
NAME2DEC["cidr"] = decode_cidr

--           oid: 651
--          name: cidr[]
--          type: base
--     type_code: b
--      category: array
-- category_code: A
OID2NAME[651] = "cidr[]"
NAME2DEC["cidr[]"] = decode_cidr_array

--           oid: 700
--          name: real
--          type: base
--     type_code: b
--     array_oid: 1021
--      category: numeric
-- category_code: N
--   description: single-precision floating point number, 4-byte storage
OID2NAME[700] = "real"
NAME2DEC["real"] = decode_real

--           oid: 701
--          name: double_precision
--          type: base
--     type_code: b
--     array_oid: 1022
--      category: numeric
-- category_code: N
--   description: double-precision floating point number, 8-byte storage
OID2NAME[701] = "double_precision"
NAME2DEC["double_precision"] = decode_double_precision

--           oid: 718
--          name: circle
--          type: base
--     type_code: b
--     array_oid: 719
--      category: geometric
-- category_code: G
--   description: geometric circle '(center,radius)'
OID2NAME[718] = "circle"
NAME2DEC["circle"] = decode_circle

--           oid: 719
--          name: circle[]
--          type: base
--     type_code: b
--      category: array
-- category_code: A
OID2NAME[719] = "circle[]"
NAME2DEC["circle[]"] = decode_circle_array

--           oid: 774
--          name: macaddr8
--          type: base
--     type_code: b
--     array_oid: 775
--      category: user_defined
-- category_code: U
--   description: XX:XX:XX:XX:XX:XX:XX:XX, MAC address
OID2NAME[774] = "macaddr8"
NAME2DEC["macaddr8"] = decode_macaddr8

--           oid: 775
--          name: macaddr8[]
--          type: base
--     type_code: b
--      category: array
-- category_code: A
OID2NAME[775] = "macaddr8[]"
NAME2DEC["macaddr8[]"] = decode_macaddr8_array

--           oid: 790
--          name: money
--          type: base
--     type_code: b
--     array_oid: 791
--      category: numeric
-- category_code: N
--   description: monetary amounts, $d,ddd.cc
OID2NAME[790] = "money"
NAME2DEC["money"] = decode_money

--           oid: 791
--          name: money[]
--          type: base
--     type_code: b
--      category: array
-- category_code: A
OID2NAME[791] = "money[]"
NAME2DEC["money[]"] = decode_money_array

--           oid: 829
--          name: macaddr
--          type: base
--     type_code: b
--     array_oid: 1040
--      category: user_defined
-- category_code: U
--   description: XX:XX:XX:XX:XX:XX, MAC address
OID2NAME[829] = "macaddr"
NAME2DEC["macaddr"] = decode_macaddr

--           oid: 869
--          name: inet
--          type: base
--     type_code: b
--     array_oid: 1041
--      category: net
-- category_code: I
--   description: IP address/netmask, host address, netmask optional
OID2NAME[869] = "inet"
NAME2DEC["inet"] = decode_inet

--           oid: 1000
--          name: boolean[]
--          type: base
--     type_code: b
--      category: array
-- category_code: A
OID2NAME[1000] = "boolean[]"
NAME2DEC["boolean[]"] = decode_boolean_array

--           oid: 1001
--          name: bytea[]
--          type: base
--     type_code: b
--      category: array
-- category_code: A
OID2NAME[1001] = "bytea[]"
NAME2DEC["bytea[]"] = decode_bytea_array

--           oid: 1002
--          name: char[]
--          type: base
--     type_code: b
--      category: array
-- category_code: A
OID2NAME[1002] = "char[]"
NAME2DEC["char[]"] = decode_char_array

--           oid: 1003
--          name: name[]
--          type: base
--     type_code: b
--      category: array
-- category_code: A
OID2NAME[1003] = "name[]"
NAME2DEC["name[]"] = decode_name_array

--           oid: 1005
--          name: smallint[]
--          type: base
--     type_code: b
--      category: array
-- category_code: A
OID2NAME[1005] = "smallint[]"
NAME2DEC["smallint[]"] = decode_smallint_array

--           oid: 1007
--          name: integer[]
--          type: base
--     type_code: b
--      category: array
-- category_code: A
OID2NAME[1007] = "integer[]"
NAME2DEC["integer[]"] = decode_integer_array

--           oid: 1009
--          name: text[]
--          type: base
--     type_code: b
--      category: array
-- category_code: A
OID2NAME[1009] = "text[]"
NAME2DEC["text[]"] = decode_text_array

--           oid: 1010
--          name: tid[]
--          type: base
--     type_code: b
--      category: array
-- category_code: A
OID2NAME[1010] = "tid[]"
NAME2DEC["tid[]"] = decode_tid_array

--           oid: 1011
--          name: xid[]
--          type: base
--     type_code: b
--      category: array
-- category_code: A
OID2NAME[1011] = "xid[]"
NAME2DEC["xid[]"] = decode_xid_array

--           oid: 1012
--          name: cid[]
--          type: base
--     type_code: b
--      category: array
-- category_code: A
OID2NAME[1012] = "cid[]"
NAME2DEC["cid[]"] = decode_cid_array

--           oid: 1014
--          name: character[]
--          type: base
--     type_code: b
--      category: array
-- category_code: A
OID2NAME[1014] = "character[]"
NAME2DEC["character[]"] = decode_character_array

--           oid: 1015
--          name: character_varying[]
--          type: base
--     type_code: b
--      category: array
-- category_code: A
OID2NAME[1015] = "character_varying[]"
NAME2DEC["character_varying[]"] = decode_character_varying_array

--           oid: 1016
--          name: bigint[]
--          type: base
--     type_code: b
--      category: array
-- category_code: A
OID2NAME[1016] = "bigint[]"
NAME2DEC["bigint[]"] = decode_bigint_array

--           oid: 1017
--          name: point[]
--          type: base
--     type_code: b
--      category: array
-- category_code: A
OID2NAME[1017] = "point[]"
NAME2DEC["point[]"] = decode_point_array

--           oid: 1018
--          name: lseg[]
--          type: base
--     type_code: b
--      category: array
-- category_code: A
OID2NAME[1018] = "lseg[]"
NAME2DEC["lseg[]"] = decode_lseg_array

--           oid: 1019
--          name: path[]
--          type: base
--     type_code: b
--      category: array
-- category_code: A
OID2NAME[1019] = "path[]"
NAME2DEC["path[]"] = decode_path_array

--           oid: 1020
--          name: box[]
--          type: base
--     type_code: b
--      category: array
-- category_code: A
OID2NAME[1020] = "box[]"
NAME2DEC["box[]"] = decode_box_array

--           oid: 1021
--          name: real[]
--          type: base
--     type_code: b
--      category: array
-- category_code: A
OID2NAME[1021] = "real[]"
NAME2DEC["real[]"] = decode_real_array

--           oid: 1022
--          name: double_precision[]
--          type: base
--     type_code: b
--      category: array
-- category_code: A
OID2NAME[1022] = "double_precision[]"
NAME2DEC["double_precision[]"] = decode_double_precision_array

--           oid: 1027
--          name: polygon[]
--          type: base
--     type_code: b
--      category: array
-- category_code: A
OID2NAME[1027] = "polygon[]"
NAME2DEC["polygon[]"] = decode_polygon_array

--           oid: 1028
--          name: oid[]
--          type: base
--     type_code: b
--      category: array
-- category_code: A
OID2NAME[1028] = "oid[]"
NAME2DEC["oid[]"] = decode_oid_array

--           oid: 1033
--          name: aclitem
--          type: base
--     type_code: b
--     array_oid: 1034
--      category: user_defined
-- category_code: U
--   description: access control list
OID2NAME[1033] = "aclitem"
NAME2DEC["aclitem"] = decode_aclitem

--           oid: 1034
--          name: aclitem[]
--          type: base
--     type_code: b
--      category: array
-- category_code: A
OID2NAME[1034] = "aclitem[]"
NAME2DEC["aclitem[]"] = decode_aclitem_array

--           oid: 1040
--          name: macaddr[]
--          type: base
--     type_code: b
--      category: array
-- category_code: A
OID2NAME[1040] = "macaddr[]"
NAME2DEC["macaddr[]"] = decode_macaddr_array

--           oid: 1041
--          name: inet[]
--          type: base
--     type_code: b
--      category: array
-- category_code: A
OID2NAME[1041] = "inet[]"
NAME2DEC["inet[]"] = decode_inet_array

--           oid: 1042
--          name: character
--          type: base
--     type_code: b
--     array_oid: 1014
--      category: string
-- category_code: S
--   description: char(length), blank-padded string, fixed storage length
OID2NAME[1042] = "character"
NAME2DEC["character"] = decode_character

--           oid: 1043
--          name: character_varying
--          type: base
--     type_code: b
--     array_oid: 1015
--      category: string
-- category_code: S
--   description: varchar(length), non-blank-padded string, variable storage length
OID2NAME[1043] = "character_varying"
NAME2DEC["character_varying"] = decode_character_varying

--           oid: 1082
--          name: date
--          type: base
--     type_code: b
--     array_oid: 1182
--      category: datetime
-- category_code: D
--   description: date
OID2NAME[1082] = "date"
NAME2DEC["date"] = decode_date

--           oid: 1083
--          name: time_without_time_zone
--          type: base
--     type_code: b
--     array_oid: 1183
--      category: datetime
-- category_code: D
--   description: time of day
OID2NAME[1083] = "time_without_time_zone"
NAME2DEC["time_without_time_zone"] = decode_time_without_time_zone

--           oid: 1114
--          name: timestamp_without_time_zone
--          type: base
--     type_code: b
--     array_oid: 1115
--      category: datetime
-- category_code: D
--   description: date and time
OID2NAME[1114] = "timestamp_without_time_zone"
NAME2DEC["timestamp_without_time_zone"] = decode_timestamp_without_time_zone

-- LuaFormatter off
--           oid: 1115
--          name: timestamp_without_time_zone[]
--          type: base
--     type_code: b
--      category: array
-- category_code: A
OID2NAME[1115] = "timestamp_without_time_zone[]"
NAME2DEC["timestamp_without_time_zone[]"] = decode_timestamp_without_time_zone_array

-- LuaFormatter on

--           oid: 1182
--          name: date[]
--          type: base
--     type_code: b
--      category: array
-- category_code: A
OID2NAME[1182] = "date[]"
NAME2DEC["date[]"] = decode_date_array

--           oid: 1183
--          name: time_without_time_zone[]
--          type: base
--     type_code: b
--      category: array
-- category_code: A
OID2NAME[1183] = "time_without_time_zone[]"
NAME2DEC["time_without_time_zone[]"] = decode_time_without_time_zone_array

--           oid: 1184
--          name: timestamp_with_time_zone
--          type: base
--     type_code: b
--     array_oid: 1185
--      category: datetime
-- category_code: D
--   description: date and time with time zone
OID2NAME[1184] = "timestamp_with_time_zone"
NAME2DEC["timestamp_with_time_zone"] = decode_timestamp_with_time_zone

--           oid: 1185
--          name: timestamp_with_time_zone[]
--          type: base
--     type_code: b
--      category: array
-- category_code: A
OID2NAME[1185] = "timestamp_with_time_zone[]"
NAME2DEC["timestamp_with_time_zone[]"] = decode_timestamp_with_time_zone_array

--           oid: 1186
--          name: interval
--          type: base
--     type_code: b
--     array_oid: 1187
--      category: timespan
-- category_code: T
--   description: @ <number> <units>, time interval
OID2NAME[1186] = "interval"
NAME2DEC["interval"] = decode_interval

--           oid: 1187
--          name: interval[]
--          type: base
--     type_code: b
--      category: array
-- category_code: A
OID2NAME[1187] = "interval[]"
NAME2DEC["interval[]"] = decode_interval_array

--           oid: 1231
--          name: numeric[]
--          type: base
--     type_code: b
--      category: array
-- category_code: A
OID2NAME[1231] = "numeric[]"
NAME2DEC["numeric[]"] = decode_numeric_array

--           oid: 1266
--          name: time_with_time_zone
--          type: base
--     type_code: b
--     array_oid: 1270
--      category: datetime
-- category_code: D
--   description: time of day with time zone
OID2NAME[1266] = "time_with_time_zone"
NAME2DEC["time_with_time_zone"] = decode_time_with_time_zone

--           oid: 1270
--          name: time_with_time_zone[]
--          type: base
--     type_code: b
--      category: array
-- category_code: A
OID2NAME[1270] = "time_with_time_zone[]"
NAME2DEC["time_with_time_zone[]"] = decode_time_with_time_zone_array

--           oid: 1560
--          name: bit
--          type: base
--     type_code: b
--     array_oid: 1561
--      category: bit
-- category_code: V
--   description: fixed-length bit string
OID2NAME[1560] = "bit"
NAME2DEC["bit"] = decode_bit

--           oid: 1561
--          name: bit[]
--          type: base
--     type_code: b
--      category: array
-- category_code: A
OID2NAME[1561] = "bit[]"
NAME2DEC["bit[]"] = decode_bit_array

--           oid: 1562
--          name: bit_varying
--          type: base
--     type_code: b
--     array_oid: 1563
--      category: bit
-- category_code: V
--   description: variable-length bit string
OID2NAME[1562] = "bit_varying"
NAME2DEC["bit_varying"] = decode_bit_varying

--           oid: 1563
--          name: bit_varying[]
--          type: base
--     type_code: b
--      category: array
-- category_code: A
OID2NAME[1563] = "bit_varying[]"
NAME2DEC["bit_varying[]"] = decode_bit_varying_array

--           oid: 1700
--          name: numeric
--          type: base
--     type_code: b
--     array_oid: 1231
--      category: numeric
-- category_code: N
--   description: numeric(precision, decimal), arbitrary precision number
OID2NAME[1700] = "numeric"
NAME2DEC["numeric"] = decode_numeric

--           oid: 1790
--          name: refcursor
--          type: base
--     type_code: b
--     array_oid: 2201
--      category: user_defined
-- category_code: U
--   description: reference to cursor (portal name)
OID2NAME[1790] = "refcursor"
NAME2DEC["refcursor"] = decode_refcursor

--           oid: 2201
--          name: refcursor[]
--          type: base
--     type_code: b
--      category: array
-- category_code: A
OID2NAME[2201] = "refcursor[]"
NAME2DEC["refcursor[]"] = decode_refcursor_array

--           oid: 2950
--          name: uuid
--          type: base
--     type_code: b
--     array_oid: 2951
--      category: user_defined
-- category_code: U
--   description: UUID datatype
OID2NAME[2950] = "uuid"
NAME2DEC["uuid"] = decode_uuid

--           oid: 2951
--          name: uuid[]
--          type: base
--     type_code: b
--      category: array
-- category_code: A
OID2NAME[2951] = "uuid[]"
NAME2DEC["uuid[]"] = decode_uuid_array

--           oid: 3614
--          name: tsvector
--          type: base
--     type_code: b
--     array_oid: 3643
--      category: user_defined
-- category_code: U
--   description: text representation for text search
OID2NAME[3614] = "tsvector"
NAME2DEC["tsvector"] = decode_tsvector

--           oid: 3615
--          name: tsquery
--          type: base
--     type_code: b
--     array_oid: 3645
--      category: user_defined
-- category_code: U
--   description: query representation for text search
OID2NAME[3615] = "tsquery"
NAME2DEC["tsquery"] = decode_tsquery

--           oid: 3643
--          name: tsvector[]
--          type: base
--     type_code: b
--      category: array
-- category_code: A
OID2NAME[3643] = "tsvector[]"
NAME2DEC["tsvector[]"] = decode_tsvector_array

--           oid: 3645
--          name: tsquery[]
--          type: base
--     type_code: b
--      category: array
-- category_code: A
OID2NAME[3645] = "tsquery[]"
NAME2DEC["tsquery[]"] = decode_tsquery_array

--           oid: 3802
--          name: jsonb
--          type: base
--     type_code: b
--     array_oid: 3807
--      category: user_defined
-- category_code: U
--   description: Binary JSON
OID2NAME[3802] = "jsonb"
NAME2DEC["jsonb"] = decode_jsonb

--           oid: 3807
--          name: jsonb[]
--          type: base
--     type_code: b
--      category: array
-- category_code: A
OID2NAME[3807] = "jsonb[]"
NAME2DEC["jsonb[]"] = decode_jsonb_array

--           oid: 3904
--          name: int4range
--          type: range
--     type_code: r
--     array_oid: 3905
--      category: range
-- category_code: R
--   description: range of integers
OID2NAME[3904] = "int4range"
NAME2DEC["int4range"] = decode_int4range

--           oid: 3905
--          name: int4range[]
--          type: base
--     type_code: b
--      category: array
-- category_code: A
OID2NAME[3905] = "int4range[]"
NAME2DEC["int4range[]"] = decode_int4range_array

--           oid: 3906
--          name: numrange
--          type: range
--     type_code: r
--     array_oid: 3907
--      category: range
-- category_code: R
--   description: range of numerics
OID2NAME[3906] = "numrange"
NAME2DEC["numrange"] = decode_numrange

--           oid: 3907
--          name: numrange[]
--          type: base
--     type_code: b
--      category: array
-- category_code: A
OID2NAME[3907] = "numrange[]"
NAME2DEC["numrange[]"] = decode_numrange_array

--           oid: 3908
--          name: tsrange
--          type: range
--     type_code: r
--     array_oid: 3909
--      category: range
-- category_code: R
--   description: range of timestamps without time zone
OID2NAME[3908] = "tsrange"
NAME2DEC["tsrange"] = decode_tsrange

--           oid: 3909
--          name: tsrange[]
--          type: base
--     type_code: b
--      category: array
-- category_code: A
OID2NAME[3909] = "tsrange[]"
NAME2DEC["tsrange[]"] = decode_tsrange_array

--           oid: 3910
--          name: tstzrange
--          type: range
--     type_code: r
--     array_oid: 3911
--      category: range
-- category_code: R
--   description: range of timestamps with time zone
OID2NAME[3910] = "tstzrange"
NAME2DEC["tstzrange"] = decode_tstzrange

--           oid: 3911
--          name: tstzrange[]
--          type: base
--     type_code: b
--      category: array
-- category_code: A
OID2NAME[3911] = "tstzrange[]"
NAME2DEC["tstzrange[]"] = decode_tstzrange_array

--           oid: 3912
--          name: daterange
--          type: range
--     type_code: r
--     array_oid: 3913
--      category: range
-- category_code: R
--   description: range of dates
OID2NAME[3912] = "daterange"
NAME2DEC["daterange"] = decode_daterange

--           oid: 3913
--          name: daterange[]
--          type: base
--     type_code: b
--      category: array
-- category_code: A
OID2NAME[3913] = "daterange[]"
NAME2DEC["daterange[]"] = decode_daterange_array

--           oid: 3926
--          name: int8range
--          type: range
--     type_code: r
--     array_oid: 3927
--      category: range
-- category_code: R
--   description: range of bigints
OID2NAME[3926] = "int8range"
NAME2DEC["int8range"] = decode_int8range

--           oid: 3927
--          name: int8range[]
--          type: base
--     type_code: b
--      category: array
-- category_code: A
OID2NAME[3927] = "int8range[]"
NAME2DEC["int8range[]"] = decode_int8range_array

--           oid: 4072
--          name: jsonpath
--          type: base
--     type_code: b
--     array_oid: 4073
--      category: user_defined
-- category_code: U
--   description: JSON path
OID2NAME[4072] = "jsonpath"
NAME2DEC["jsonpath"] = decode_jsonpath

--           oid: 4073
--          name: jsonpath[]
--          type: base
--     type_code: b
--      category: array
-- category_code: A
OID2NAME[4073] = "jsonpath[]"
NAME2DEC["jsonpath[]"] = decode_jsonpath_array

--           oid: 4451
--          name: int4multirange
--          type: multirange
--     type_code: m
--     array_oid: 6150
--      category: range
-- category_code: R
--   description: multirange of integers
OID2NAME[4451] = "int4multirange"
NAME2DEC["int4multirange"] = decode_int4multirange

--           oid: 4532
--          name: nummultirange
--          type: multirange
--     type_code: m
--     array_oid: 6151
--      category: range
-- category_code: R
--   description: multirange of numerics
OID2NAME[4532] = "nummultirange"
NAME2DEC["nummultirange"] = decode_nummultirange

--           oid: 4533
--          name: tsmultirange
--          type: multirange
--     type_code: m
--     array_oid: 6152
--      category: range
-- category_code: R
--   description: multirange of timestamps without time zone
OID2NAME[4533] = "tsmultirange"
NAME2DEC["tsmultirange"] = decode_tsmultirange

--           oid: 4534
--          name: tstzmultirange
--          type: multirange
--     type_code: m
--     array_oid: 6153
--      category: range
-- category_code: R
--   description: multirange of timestamps with time zone
OID2NAME[4534] = "tstzmultirange"
NAME2DEC["tstzmultirange"] = decode_tstzmultirange

--           oid: 4535
--          name: datemultirange
--          type: multirange
--     type_code: m
--     array_oid: 6155
--      category: range
-- category_code: R
--   description: multirange of dates
OID2NAME[4535] = "datemultirange"
NAME2DEC["datemultirange"] = decode_datemultirange

--           oid: 4536
--          name: int8multirange
--          type: multirange
--     type_code: m
--     array_oid: 6157
--      category: range
-- category_code: R
--   description: multirange of bigints
OID2NAME[4536] = "int8multirange"
NAME2DEC["int8multirange"] = decode_int8multirange

--           oid: 5069
--          name: xid8
--          type: base
--     type_code: b
--     array_oid: 271
--      category: user_defined
-- category_code: U
--   description: full transaction id
OID2NAME[5069] = "xid8"
NAME2DEC["xid8"] = decode_xid8

--           oid: 6150
--          name: int4multirange[]
--          type: base
--     type_code: b
--      category: array
-- category_code: A
OID2NAME[6150] = "int4multirange[]"
NAME2DEC["int4multirange[]"] = decode_int4multirange_array

--           oid: 6151
--          name: nummultirange[]
--          type: base
--     type_code: b
--      category: array
-- category_code: A
OID2NAME[6151] = "nummultirange[]"
NAME2DEC["nummultirange[]"] = decode_nummultirange_array

--           oid: 6152
--          name: tsmultirange[]
--          type: base
--     type_code: b
--      category: array
-- category_code: A
OID2NAME[6152] = "tsmultirange[]"
NAME2DEC["tsmultirange[]"] = decode_tsmultirange_array

--           oid: 6153
--          name: tstzmultirange[]
--          type: base
--     type_code: b
--      category: array
-- category_code: A
OID2NAME[6153] = "tstzmultirange[]"
NAME2DEC["tstzmultirange[]"] = decode_tstzmultirange_array

--           oid: 6155
--          name: datemultirange[]
--          type: base
--     type_code: b
--      category: array
-- category_code: A
OID2NAME[6155] = "datemultirange[]"
NAME2DEC["datemultirange[]"] = decode_datemultirange_array

--           oid: 6157
--          name: int8multirange[]
--          type: base
--     type_code: b
--      category: array
-- category_code: A
OID2NAME[6157] = "int8multirange[]"
NAME2DEC["int8multirange[]"] = decode_int8multirange_array

--- @class postgres.decoder
--- @field oid2name table<integer, string> oid to name
--- @field name2dec table<string, function> name to decode function
local Decoder = {}

--- init
--- @return postgres.decoder
function Decoder:init()
    self.oid2name = {}
    self.name2dec = {}
    for oid, name in pairs(OID2NAME) do
        self.oid2name[oid] = name
    end
    for name, decodefn in pairs(NAME2DEC) do
        self.name2dec[name] = decodefn
    end
    return self
end

--- register_name2dec registers a decoder function for a type name
--- @param name string
--- @param decodefn function
function Decoder:register_name2dec(name, decodefn)
    assert(type(name) == 'string', "name must be string")
    assert(type(decodefn) == 'function', "decodefn must be function")
    self.name2dec[name] = decodefn
end

--- register_oid2name registers an oid to type name mapping
--- @param oid integer
--- @param name string
function Decoder:register_oid2name(oid, name)
    assert(type(oid) == 'number', "oid must be integer")
    assert(type(name) == 'string', "name must be string")
    assert(self.name2dec[name], "name is not registered")
    self.oid2name[oid] = name
end

--- register registers a decoder function for a type oid and name
--- @param oid integer
--- @param name string
--- @param decodefn function
function Decoder:register(oid, name, decodefn)
    self:register_name2dec(name, decodefn)
    self:register_oid2name(oid, name)
end

--- decode decodes a field value
--- @param oid integer
--- @param s string
--- @return any value
--- @return any error
function Decoder:decode(oid, s)
    local name = self.oid2name[oid]
    local decodefn = name and self.name2dec[name]
    if decodefn then
        return decodefn(s)
    end
    return s
end

return {
    new = require('metamodule').new(Decoder),
}
