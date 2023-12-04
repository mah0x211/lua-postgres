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
local type = type
local errorf = require('error').format
local htonl = require('postgres.htonl')
local ntohl = require('postgres.ntohl')

--- @class postgres.message.cancel_request : postgres.message
--- @field pid integer
--- @field key integer
local CancelRequest = require('metamodule').new({}, 'postgres.message')

--
-- CancelRequest (F)
--   Int32(16)
--     Length of message contents in bytes, including self.
--
--   Int32(80877102)
--     The cancel request code. The value is chosen to contain 1234 in the
--     most significant 16 bits, and 5678 in the least significant 16 bits.
--     (To avoid confusion, this code must not be the same as any protocol
--      version number.)
--
--   Int32
--     The process ID of the target backend.
--
--   Int32
--     The secret key for the target backend.
--

--- decode
--- @param s string
--- @return postgres.message.cancel_request?
--- @return any err
--- @return boolean? again
local function decode(s)
    if #s < 4 then
        return nil, nil, true
    end

    local len = ntohl(s)
    if len < 16 then
        return nil, errorf('invalid CancelRequest message: length must be 16')
    elseif #s < 8 then
        return nil, nil, true
    end

    local code = ntohl(sub(s, 5))
    if code ~= 80877102 then
        return nil,
               errorf('invalid CancelRequest message: code must be 80877102')
    elseif #s < len then
        return nil, nil, true
    end

    local msg = CancelRequest()
    msg.consumed = 16
    msg.pid = ntohl(sub(s, 9))
    msg.key = ntohl(sub(s, 13))
    return msg
end

--- encode
--- @param pid integer
--- @param key integer
--- @return string
local function encode(pid, key)
    assert(type(pid) == 'number', 'pid must be integer')
    assert(type(key) == 'number', 'key must be integer')
    return htonl(16) .. htonl(80877102) .. htonl(pid) .. htonl(key)
end

return {
    encode = encode,
    decode = decode,
}
