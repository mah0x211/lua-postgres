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
local rep = string.rep
local errorf = require('error').format
local unpack = require('postgres.unpack')

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

--- @class postgres.message.data_row : postgres.message
--- @field values string[]
local DataRow = require('metamodule').new({}, 'postgres.message')

--- decode
--- @param s string
--- @return postgres.message.data_row? msg
--- @return any err
--- @return boolean? again
local function decode(s)
    if #s < 1 then
        return nil, nil, true
    elseif sub(s, 1, 1) ~= 'D' then
        return nil, errorf('invalid DataRow message')
    elseif #s < 5 then
        return nil, nil, true
    end

    --
    -- decode the following fields
    --   Byte1('D')
    --     Identifies the message as a data row.
    --
    --   Int32
    --     Length of message contents in bytes, including self.
    --
    --   Int16
    --     The number of column values that follow (possibly zero).
    --
    local v = {}
    local consumed, err, again = unpack(v, 'b1Lh', s)
    if err then
        return nil, errorf('invalid DataRow message', err)
    elseif again then
        if v[2] < 6 then
            return nil, errorf(
                       'invalid DataRow message: length is not greater than 5')
        end
        return nil, nil, true
    end

    local msg = DataRow()
    msg.consumed = v[2] + 1 -- +1 for the Byte1 field
    msg.type = 'DataRow'
    msg.values = {}

    -- extract remaining message body
    s = sub(s, consumed + 1, msg.consumed)

    --
    -- convert column values
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
    local ncol = v[3]
    if ncol < 0 then
        return nil, errorf(
                   'invalid DataRow message: number of column values is not greater than or equal to 0')
    elseif ncol > 0 then
        v = {}
        local _
        consumed, _, again = unpack(v, rep('ib*', ncol), s)
        if again then
            return nil, errorf(
                       'invalid DataRow message: message length is not enough to decode column values')
        end
        s = sub(s, consumed + 1)

        local values = msg.values
        local k = 1
        for i = 1, #v, 2 do
            if v[i] < -1 then
                return nil,
                       errorf(
                           'invalid DataRow message: column value#%d length %d is not supported',
                           i, v[i])
            elseif v[i] == 0 then
                values[k] = ''
            elseif v[i] ~= -1 then
                values[k] = v[i + 1]
            end
            k = k + 1
        end
    end

    -- check the remaining message length
    if #s > 0 then
        return nil, errorf(
                   'invalid DataRow message: message length is too long (unknown %d bytes of data remains)',
                   #s)
    end

    return msg
end

return {
    decode = decode,
}
