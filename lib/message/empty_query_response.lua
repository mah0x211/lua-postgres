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

--- @class postgres.message.empty_query_response : postgres.message
local EmptyQueryResponse = require('metamodule').new({}, 'postgres.message')

--- decode
--- @param s string
--- @return postgres.message.empty_query_response? msg
--- @return any err
--- @return boolean? again
local function decode(s)
    --
    -- EmptyQueryResponse (B)
    --   Byte1('I')
    --     Identifies the message as a response to an empty query string.
    --     (This substitutes for CommandComplete.)
    --
    --   Int32(4)
    --     Length of message contents in bytes, including self.
    --
    if #s < 1 then
        return nil, nil, true
    elseif sub(s, 1, 1) ~= 'I' then
        return nil, errorf('invalid EmptyQueryResponse message')
    elseif #s < 5 then
        return nil, nil, true
    end

    local len = ntohl(sub(s, 2))
    if len ~= 4 then
        return nil,
               errorf('invalid EmptyQueryResponse message: length must be 4')
    end

    local msg = EmptyQueryResponse()
    msg.consumed = len + 1 -- +1 for the Byte1 field
    msg.type = 'EmptyQueryResponse'
    return msg
end

return {
    decode = decode,
}
