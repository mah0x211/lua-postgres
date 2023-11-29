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
local htonl = require('postgres.htonl')

--- @class postgres.message.backend_key_data : postgres.message
--- @field pid integer
--- @field key integer
local BackendKeyData = require('metamodule').new({}, 'postgres.message')

--- decode
--- @param s string
--- @return table? msg
--- @return any err
--- @return boolean? again
local function decode(s)
    --
    -- BackendKeyData (B)
    --   Byte1('K')
    --     Identifies the message as cancellation key data. The frontend must
    --     save these values if it wishes to be able to issue CancelRequest
    --     messages later.
    --
    --   Int32(12)
    --     Length of message contents in bytes, including self.
    --
    --   Int32
    --     The process ID of this backend.
    --
    --   Int32
    --     The secret key of this backend.
    --
    if #s < 1 then
        return nil, nil, true
    elseif sub(s, 1, 1) ~= 'K' then
        return nil, errorf('invalid BackendKeyData message')
    elseif #s < 5 then
        return nil, nil, true
    end

    local len = ntohl(sub(s, 2))
    local consumed = len + 1
    if len ~= 12 then
        return nil, errorf('invalid BackendKeyData message')
    elseif #s < consumed then
        return nil, nil, true
    end

    local msg = BackendKeyData()
    msg.consumed = consumed
    msg.type = 'BackendKeyData'
    msg.pid = ntohl(sub(s, 6))
    msg.key = ntohl(sub(s, 10))
    return msg
end

--- encode
--- @param pid integer
--- @param key integer
--- @return string
local function encode(pid, key)
    return 'K' .. htonl(12) .. htonl(pid) .. htonl(key)
end

return {
    encode = encode,
    decode = decode,
}
