--
-- Copyright (C) 2025 Masatoshi Fukunaga
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
local htonl = require('postgres.htonl')
local ntohl = require('postgres.ntohl')
local errorf = require('error').format

--
-- CopyFail (F)
--   Byte1('f')
--     Identifies the message as a COPY-failure indicator.
--
--   Int32 (4 bytes)
--     Length of message contents in bytes, including self.
--
--   String
--     Error message describing why the COPY operation failed.
--
--   This message is sent from a frontend to the backend to indicate that
--   a COPY operation has failed and provides an error message explaining
--   the reason for the failure.
--

--- @class postgres.message.copy_fail : postgres.message
--- @field message string error message describing the failure
local CopyFail = require('metamodule').new({}, 'postgres.message')

--- encode
--- @param errmsg string error message describing why COPY failed
--- @return string msg
local function encode(errmsg)
    return 'f' .. htonl(4 + #errmsg) .. errmsg
end

--- decode
--- @param s string
--- @return table? msg
--- @return any err
--- @return boolean? again
local function decode(s)
    if #s < 5 then
        return nil, nil, true
    elseif sub(s, 1, 1) ~= 'f' then
        return nil, errorf('invalid CopyFail message')
    end

    local len = ntohl(sub(s, 2, 5))
    local consumed = len + 1
    if #s < consumed then
        -- need more data
        return nil, nil, true
    end

    -- Extract error message (data starts after length field)
    local errmsg = sub(s, 6, consumed)

    local msg = CopyFail()
    msg.consumed = consumed
    msg.type = 'CopyFail'
    msg.message = errmsg

    return msg
end

return {
    encode = encode,
    decode = decode,
}
