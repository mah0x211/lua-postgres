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
local type = type
local sub = string.sub
local rep = string.rep
local format = string.format
local concat = table.concat
local htonl = require('postgres.htonl')
local htons = require('postgres.htons')
local ntohs = require('postgres.ntohs')
local unpack = require('postgres.unpack')
local errorf = require('error').format
--- constants
local NULL = '\0'
local FORMAT_NAMES = {
    [0] = 'text',
    [1] = 'binary',
}

--
-- Bind (F)
--   Byte1('B')
--     Identifies the message as a Bind command.
--
--   Int32
--     Length of message contents in bytes, including self.
--
--   String
--     The name of the destination portal (an empty string selects the
--     unnamed portal).
--
--   String
--     The name of the source prepared statement (an empty string selects
--     the unnamed prepared statement).
--
--   Int16
--     The number of parameter format codes that follow (denoted C below).
--     This can be zero to indicate that there are no parameters or that the
--     parameters all use the default format (text); or one, in which case
--     the specified format code is applied to all parameters; or it can
--     equal the actual number of parameters.
--
--   Int16[C]
--     The parameter format codes. Each must presently be zero (text) or
--     one (binary).
--
--   Int16
--     The number of parameter values that follow (possibly zero). This must
--     match the number of parameters needed by the query.
--
-- Next, the following pair of fields appear for each parameter:
--
--   Int32
--     The length of the parameter value, in bytes (this count does not
--     include itself). Can be zero. As a special case, -1 indicates a NULL
--     parameter value. No value bytes follow in the NULL case.
--
--   Byten
--     The value of the parameter, in the format indicated by the associated
--     format code. n is the above length.
--
-- After the last parameter, the following fields appear:
--
--   Int16
--     The number of result-column format codes that follow (denoted R
--     below). This can be zero to indicate that there are no result columns
--     or that the result columns should all use the default format (text);
--     or one, in which case the specified format code is applied to all
--     result columns (if any); or it can equal the actual number of result
--     columns of the query.
--
--   Int16[R]
--     The result-column format codes. Each must presently be zero (text) or
--     one (binary).
--

--- @class postgres.message.bind : postgres.message
--- @field portal string?
--- @field stmt string?
--- @field formats string[] -- 0:text, 1:binary, if empty then format is text
--- @field values string[]  -- byte string that format is specified by formats
--- @field results string[] -- 0:text, 1:binary, if empty then format is text
local Bind = require('metamodule').new({}, 'postgres.message')

--- decode
--- @param s string
--- @return postgres.message.bind? msg
--- @return any err
--- @return boolean? again
local function decode(s)
    --
    --   Byte1('B')
    --     Identifies the message as a Bind command.
    --
    --   Int32
    --     Length of message contents in bytes, including self.
    --
    if #s < 1 then
        return nil, nil, true
    elseif sub(s, 1, 1) ~= 'B' then
        return nil, errorf('invalid Bind message')
    elseif #s < 5 then
        return nil, nil, true
    end

    --
    -- decode the following fields
    --   Byte1('B')
    --     Identifies the message as a Bind command.
    --
    --   Int32
    --     Length of message contents in bytes, including self.
    --
    --   String
    --     The name of the destination portal (an empty string selects the
    --     unnamed portal).
    --
    --   String
    --     The name of the source prepared statement (an empty string selects
    --     the unnamed prepared statement).
    --
    --   Int16
    --     The number of parameter format codes that follow (denoted C below).
    --     This can be zero to indicate that there are no parameters or that the
    --     parameters all use the default format (text); or one, in which case
    --     the specified format code is applied to all parameters; or it can
    --     equal the actual number of parameters.
    --
    --   Int16[C]
    --     The parameter format codes. Each must presently be zero (text) or
    --     one (binary).
    --
    local v = {}
    local consumed, err, again = unpack(v, 'b1Lsshh*', s)
    if err then
        return nil, errorf('invalid Bind message', err)
    elseif again then
        -- message length must be least sum of the following fields;
        --  Int32  : 4 (message length)
        --  String : length + 1 (null-terminated) (portal)
        --  String : length + 1 (null-terminated) (stmt)
        --  Int16  : 2 (number of parameter format codes)
        --  Int16  : 2 (number of parameter values)
        --  Int16  : 2 (number of result-column format codes)
        if v[2] < 12 then
            return nil, errorf(
                       'invalid Bind message: message length must be greater than or equal to 12')
        end
        return nil, nil, true
    end

    local msg = Bind()
    msg.consumed = v[2] + 1 -- +1 for Byte1
    msg.type = 'Bind'
    msg.portal = v[3]
    msg.stmt = v[4]
    msg.formats = {}
    msg.values = {}
    msg.results = {}

    -- extract remaining message body
    s = sub(s, consumed + 1, msg.consumed)

    --
    -- convert parameter format codes to format names
    --   Int16
    --     The number of parameter format codes that follow (denoted C below).
    --     This can be zero to indicate that there are no parameters or that the
    --     parameters all use the default format (text); or one, in which case
    --     the specified format code is applied to all parameters; or it can
    --     equal the actual number of parameters.
    --
    --   Int16[C]
    --     The parameter format codes. Each must presently be zero (text) or
    --     one (binary).
    --
    local n = v[5]
    if n < 0 then
        return nil, errorf(
                   'invalid Bind message: number of parameter format codes must be greater than or equal to 0')
    elseif n > 0 then
        local formats = msg.formats
        for i = 1, n do
            local code = v[5 + i]
            local name = FORMAT_NAMES[code]
            if not name then
                return nil,
                       errorf(
                           'invalid Bind message: parameter format#%d code %d is not supported',
                           i, code)
            end
            formats[i] = name
        end
    end

    --
    -- decode parameter values
    --   Int16
    --     The number of parameter values that follow (possibly zero). This must
    --     match the number of parameters needed by the query.
    --
    -- Next, the following pair of fields appear for each parameter:
    --
    --   Int32
    --     The length of the parameter value, in bytes (this count does not
    --     include itself). Can be zero. As a special case, -1 indicates a NULL
    --     parameter value. No value bytes follow in the NULL case.
    --
    --   Byten
    --     The value of the parameter, in the format indicated by the associated
    --     format code. n is the above length.
    --
    if #s < 2 then
        -- not enough to decode number of parameter values
        return nil, errorf(
                   'invalid Bind message: message length is not enough to decode number of parameter values')
    end
    n = ntohs(s)
    s = sub(s, 3)
    if n < 0 then
        return nil, errorf(
                   'invalid Bind message: number of parameter values must be greater than or equal to 0')
    elseif n > 0 then
        v = {}
        local _
        consumed, _, again = unpack(v, rep('ib*', n), s)
        if again then
            return nil, errorf(
                       'invalid Bind message: message length is not enough to decode parameter values')
        end
        s = sub(s, consumed + 1)

        local values = msg.values
        local k = 1
        for i = 1, #v, 2 do
            if v[i] < -1 then
                return nil,
                       errorf(
                           'invalid Bind message: parameter value#%d length %d is not supported',
                           i, v[i])
            elseif v[i] == 0 then
                values[k] = ''
            elseif v[i] ~= -1 then
                values[k] = v[i + 1]
            end
            k = k + 1
        end
    end

    --
    -- decode result-column format codes to format names
    -- After the last parameter, the following fields appear:
    --
    --   Int16
    --     The number of result-column format codes that follow (denoted R
    --     below). This can be zero to indicate that there are no result columns
    --     or that the result columns should all use the default format (text);
    --     or one, in which case the specified format code is applied to all
    --     result columns (if any); or it can equal the actual number of result
    --     columns of the query.
    --
    --   Int16[R]
    --     The result-column format codes. Each must presently be zero (text) or
    --     one (binary).
    --
    if #s < 2 then
        -- not enough to decode number of result format codes
        return nil, errorf(
                   'invalid Bind message: message length is not enough to decode number of result-column format codes')
    end
    n = ntohs(s)
    s = sub(s, 3)
    if n < 0 then
        return nil, errorf(
                   'invalid Bind message: number of result-column format codes must be greater than or equal to 0')
    elseif n * 2 > #s then
        return nil, errorf(
                   'invalid Bind message: message length is not enough to decode result-column format codes')
    elseif n > 0 then
        v = {}
        consumed = unpack(v, rep('h', n), s)
        s = sub(s, consumed + 1)

        local results = msg.results
        for i = 1, n do
            local code = v[i]
            local name = FORMAT_NAMES[code]
            if not name then
                return nil, errorf(
                           'invalid Bind message: result-column format#%d code %d is not supported',
                           i, code)
            end
            results[i] = name
        end
    end

    -- check the remaining message length
    if #s > 0 then
        return nil, errorf(
                   'invalid Bind message: message length is too long (unknown %d bytes of data remains)',
                   #s)
    end

    return msg
end

--- encode
--- @param portal string
--- @param stmt string
--- @param values string[]
--- @return string
local function encode(portal, stmt, values)
    assert(type(portal) == 'string', 'portal must be string')
    assert(type(stmt) == 'string', 'stmt must be string')
    assert(type(values) == 'table', 'values must be table')

    local tbl = {
        portal,
        NULL,
        stmt,
        NULL,
        htons(0), -- number of parameter format codes
    }
    tbl[#tbl + 1] = htons(#values) -- number of parameter values
    for i = 1, #values do
        if type(values[i]) ~= 'string' then
            error(format('values#%d must be string', i))
        end
        tbl[#tbl + 1] = htonl(#values[i])
        tbl[#tbl + 1] = values[i]
    end
    tbl[#tbl + 1] = htons(0) -- all result columns use the default format (text)

    local msg = concat(tbl)
    return 'B' .. htonl(#msg + 4) .. msg
end

return {
    encode = encode,
    decode = decode,
}
