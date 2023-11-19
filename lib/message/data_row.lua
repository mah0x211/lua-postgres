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
local sub = string.sub
local errorf = require('error').format
local ntohl = require('postgres.ntohl')
local ntohs = require('postgres.ntohs')

--- @class postgres.message.data_row : postgres.message
--- @field values string[]
local DataRow = require('metamodule').new({}, 'postgres.message')

--- decode
--- @param s string
--- @return table? msg
--- @return any err
--- @return boolean? again
local function decode(s)
    --
    -- DataRow (B)
    --   Byte1('D')
    --     Identifies the message as a data row.
    --
    --   Int32
    --     Length of message contents in bytes, including self.
    --
    --   Int16
    --     The number of column values that follow (possibly zero).
    --
    --   Next, the following pair of fields appear for each column:
    --
    --   Int32
    --     The length of the column value, in bytes (this count does not include
    --     itself). Can be zero. As a special case, -1 indicates a NULL column
    --     value. No value bytes follow in the NULL case.
    --
    --   Byten
    --     The value of the column, in the format indicated by the associated
    --     format code. n is the above length.
    --
    if #s < 5 then
        return nil, nil, true
    elseif sub(s, 1, 1) ~= 'D' then
        return nil, errorf('invalid DataRow message')
    end

    local len = ntohl(sub(s, 2))
    local consumed = len + 1
    if #s < consumed then
        return nil, nil, true
    end

    local ncol = ntohs(sub(s, 6))
    local values = {}
    local head = 8
    for i = 1, ncol do
        -- length of the column value
        local vlen = ntohl(sub(s, head))
        head = head + 4
        if vlen == -1 then
            values[i] = 'NULL'
        else
            -- non-null column value
            local tail = head + vlen - 1
            if tail > consumed then
                return nil, errorf('invalid DataRow message')
            end
            values[i] = sub(s, head, tail)
            head = tail + 1
        end
    end

    local msg = DataRow()
    msg.consumed = consumed
    msg.type = 'DataRow'
    msg.values = values
    return msg
end

return {
    decode = decode,
}
