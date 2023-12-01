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
local concat = table.concat
local errorf = require('error').format
local htonl = require('postgres.htonl')
local ntohl = require('postgres.ntohl')
local unpack = require('postgres.unpack')
--- constants
local NULL = '\0'

--
-- Describe (F)
--   Byte1('D')
--     Identifies the message as a Describe command.
--
--   Int32
--     Length of message contents in bytes, including self.
--
--   Byte1
--     'S' to describe a prepared statement; or 'P' to describe a portal.
--
--   String
--     The name of the prepared statement or portal to describe (an empty
--     string selects the unnamed prepared statement or portal).
--

--- @class postgres.message.describe : postgres.message
--- @field name string
local Describe = require('metamodule').new({}, 'postgres.message')

--- decode
--- @param s string
--- @return postgres.message.describe? msg
--- @return any err
--- @return boolean? again
local function decode(s)
    if #s < 1 then
        return nil, nil, true
    elseif s:sub(1, 1) ~= 'D' then
        return nil, errorf('invalid Describe message')
    elseif #s < 5 then
        return nil, nil, true
    end

    local len = ntohl(sub(s, 2))
    if len < 6 then
        return nil, errorf(
                   'invalid Describe message: length must be greater than 6')
    elseif #s < len then
        return nil, nil, true
    end

    local v = {}
    local consumed, err = unpack(v, 'b1Lb1s', s)
    if err then
        return nil, errorf('invalid Describe message: %s', err)
    end

    local ident
    if v[3] == 'S' then
        ident = 'DescribeStatement'
    elseif v[3] == 'P' then
        ident = 'DescribePortal'
    else
        return nil, errorf('invalid Describe message: target is not "S" or "P"')
    end

    local msg = Describe()
    msg.consumed = consumed
    msg.type = ident
    msg.name = v[4]
    return msg
end

--- encode
--- @param target string # target must be 'statement' or 'portal'
--- @param name string # name of the prepared statement or portal to describe
--- @return string s
local function encode(target, name)
    assert(type(target) == 'string' and
               (target == 'statement' or target == 'portal'),
           'target must be "statement" or "portal"')
    assert(type(name) == 'string', 'portal must be string')

    local s = concat({
        target == 'statement' and 'S' or 'P',
        name,
        NULL,
    })
    return 'D' .. htonl(4 + #s) .. s
end

return {
    encode = encode,
    decode = decode,
}
