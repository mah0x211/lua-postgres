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
--- assign to local
local find = string.find
local sub = string.sub
local errorf = require('error').format
local ntohl = require('postgres.ntohl')
local ntohs = require('postgres.ntohs')
local new_rows = require('postgres.rows').new
--- constants
local NULL = '\0'

--- @class postgres.message.row_description.field
--- @field col integer
--- @field name string
--- @field table_oid integer
--- @field table_col integer
--- @field format string 'text' | 'binary'
--- @field type_oid integer
--- @field size integer
--- @field modifier integer

--- @class postgres.message.row_description : postgres.message
--- @field fields postgres.message.row_description.field[]
local RowDescription = {}

--- get_rows
--- @return postgres.rows? rows
function RowDescription:get_rows()
    return new_rows(self.conn, self.fields)
end

RowDescription = require('metamodule').new(RowDescription, 'postgres.message')

--- decode
--- @param s string
--- @return table? msg
--- @return any err
--- @return boolean? again
local function decode(s)
    --
    -- RowDescription (B)
    --   Byte1('T')
    --     Identifies the message as a row description.
    --
    --   Int32
    --     Length of message contents in bytes, including self.
    --
    --   Int16
    --     Specifies the number of fields in a row (can be zero).
    --
    --   Then, for each field, there is the following:
    --
    --   String
    --     The field name.
    --
    --   Int32
    --     If the field can be identified as a column of a specific table,
    --     the object ID of the table; otherwise zero.
    --
    --   Int16
    --     If the field can be identified as a column of a specific table, the
    --     attribute number of the column; otherwise zero.
    --
    --   Int32
    --     The object ID of the field's data type.
    --
    --   Int16
    --     The data type size (see pg_type.typlen). Note that negative values
    --     denote variable-width types.
    --
    --   Int32
    --     The type modifier (see pg_attribute.atttypmod). The meaning of the
    --     modifier is type-specific.
    --
    --   Int16
    --     The format code being used for the field. Currently will be zero
    --     (text) or one (binary). In a RowDescription returned from the
    --     statement variant of Describe, the format code is not yet known and
    --     will always be zero.
    --
    if #s < 5 then
        return nil, nil, true
    elseif sub(s, 1, 1) ~= 'T' then
        return nil, errorf('invalid RowDescription message')
    end

    local len = ntohl(sub(s, 2))
    local consumed = len + 1
    if #s < consumed then
        return nil, nil, true
    end

    local nfield = ntohs(sub(s, 6))
    local fields = {} --- @type postgres.message.row_description.field[]
    local head = 8
    for i = 1, nfield do
        local tail = find(s, NULL, head, true)
        if not tail or tail > consumed then
            return nil, errorf('invalid RowDescription message')
        end

        -- field name
        local name = sub(s, head, tail - 1)
        head = tail + 1
        -- table object ID
        local table_oid = ntohl(sub(s, head))
        head = head + 4
        -- attribute number of the column
        local table_col = ntohs(sub(s, head))
        head = head + 2
        -- object ID of the field's data type
        local type_oid = ntohl(sub(s, head))
        head = head + 4
        -- data type size
        local size = ntohs(sub(s, head))
        head = head + 2
        -- type modifier
        local modifier = ntohl(sub(s, head))
        head = head + 4
        -- format code
        local format = ntohs(sub(s, head))
        head = head + 2
        fields[i] = {
            col = i,
            name = name,
            table_oid = table_oid,
            table_col = table_col,
            format = format == 0 and 'text' or 'binary',
            type_oid = type_oid,
            size = size,
            modifier = modifier,
        }
        fields[name] = fields[i]
    end

    local msg = RowDescription()
    msg.consumed = consumed
    msg.type = 'RowDescription'
    msg.fields = fields
    return msg
end

return {
    decode = decode,
}
