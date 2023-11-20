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

--- @class postgres.message.error_response : postgres.message
--- @field severity string
--- @field code string
--- @field message string
--- @field detail string
--- @field hint string
--- @field position integer
--- @field internal_position integer
--- @field internal_query string
--- @field where string
--- @field schema_name string
--- @field table_name string
--- @field column_name string
--- @field data_type_name string
--- @field constraint_name string
--- @field file string
--- @field line integer
--- @field routine string
local ErrorResponse = require('metamodule').new({}, 'postgres.message')

--- constants
local NULL = '\0'
local ERROR_FIELDS = {
    S = 'severity',
    V = 'severity',
    C = 'code',
    M = 'message',
    D = 'detail',
    H = 'hint',
    P = 'position',
    p = 'internal_position',
    q = 'internal_query',
    W = 'where',
    s = 'schema_name',
    t = 'table_name',
    c = 'column_name',
    d = 'data_type_name',
    n = 'constraint_name',
    F = 'file',
    L = 'line',
    R = 'routine',
}

--- error_fields
--- @param msg postgres.message.error_response
--- @param s string
--- @param head integer
--- @return boolean ok
--- @return any err
local function error_fields(msg, s, head)
    while true do
        local code = sub(s, head, head)
        if code == NULL then
            -- found terminator
            if head - msg.consumed ~= 0 then
                -- message is not consumed correctly
                return false, errorf('invalid error response message')
            end
            return true
        end

        local field = ERROR_FIELDS[code]
        if not field then
            -- unknown field code
            return false, errorf(
                       'invalid error response message: unknown field code %q',
                       code)
        end

        -- find null-terminated string
        head = head + 1
        local tail = find(s, NULL, head, true)
        if not tail or tail > msg.consumed then
            -- invalid error response message
            return false, errorf('invalid error response message')
        end
        msg[field] = sub(s, head, tail - 1)
        head = tail + 1
    end
end

--- decode
--- @param s string
--- @return table? msg
--- @return any err
--- @return boolean? again
local function decode(s)
    --
    -- ErrorResponse (B)
    --   Byte1('E')
    --     Identifies the message as an error.
    --
    --   Int32
    --     Length of message contents in bytes, including self.
    --
    -- The message body consists of one or more identified fields, followed by
    -- a zero byte as a terminator. Fields can appear in any order. For each
    -- field there is the following:
    --
    --   Byte1
    --     A code identifying the field type; if zero, this is the message
    --     terminator and no string follows. The presently defined field types
    --     are listed in Section 55.8. Since more field types might be added in
    --     future, frontends should silently ignore fields of unrecognized type.
    --
    --   String
    --     The field value.
    --
    --
    -- NoticeResponse (B)
    --   Byte1('N')
    --     Identifies the message as a notice.
    --
    --   Same as the ErrorResponse message format.
    --
    if #s < 5 then
        return nil, nil, true
    end

    local ident = sub(s, 1, 1)
    if ident == 'E' then
        ident = 'ErrorResponse'
    elseif ident == 'N' then
        ident = 'NoticeResponse'
    else
        return nil, errorf('invalid ErrorResponse/NoticeResponse message')
    end

    local len = ntohl(sub(s, 2))
    local consumed = len + 1
    if #s < consumed then
        return nil, nil, true
    end

    local msg = ErrorResponse()
    msg.consumed = consumed
    msg.type = ident
    local ok, err = error_fields(msg, s, 6)
    if not ok then
        return nil, err
    end
    return msg
end

return {
    decode = decode,
}
